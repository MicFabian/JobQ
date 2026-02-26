package com.jobq.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.jobq.Job;
import com.jobq.JobRepository;
import com.jobq.JobWorker;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(prefix = "jobq.background-job-server", name = "enabled", havingValue = "true", matchIfMissing = true)
public class JobPoller {

    private static final Logger log = LoggerFactory.getLogger(JobPoller.class);

    private final JobRepository jobRepository;
    private final List<JobWorker<?>> workers;
    private final ListableBeanFactory beanFactory;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final com.jobq.config.JobQProperties properties;
    private final int workerCount;
    private final ThreadPoolExecutor processingExecutor;
    private final ThreadPoolExecutor pollingExecutor;

    private final String nodeId = "node-" + UUID.randomUUID().toString();
    private Map<String, RegisteredJob> jobMap = Map.of();
    private Map<String, AtomicBoolean> pollInProgress = Map.of();

    public JobPoller(
            JobRepository jobRepository,
            List<JobWorker<?>> workers,
            ListableBeanFactory beanFactory,
            ObjectMapper objectMapper,
            TransactionTemplate transactionTemplate,
            com.jobq.config.JobQProperties properties) {
        this.jobRepository = jobRepository;
        this.workers = workers;
        this.beanFactory = beanFactory;
        this.objectMapper = objectMapper;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
        this.workerCount = Math.max(1, properties.getBackgroundJobServer().getWorkerCount());

        int processingQueueCapacity = Math.max(32, workerCount * 8);
        this.processingExecutor = new ThreadPoolExecutor(
                workerCount,
                workerCount,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(processingQueueCapacity),
                new ThreadPoolExecutor.CallerRunsPolicy());

        int pollThreads = Math.min(4, Math.max(1, workerCount / 2));
        int pollingQueueCapacity = Math.max(32, workerCount * 4);
        this.pollingExecutor = new ThreadPoolExecutor(
                pollThreads,
                pollThreads,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(pollingQueueCapacity),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @PostConstruct
    public void init() {
        Map<String, RegisteredJob> registrations = new LinkedHashMap<>();

        for (JobWorker<?> worker : workers) {
            registerWorkerBasedJob(registrations, worker);
        }

        Map<String, Object> annotationBeans = beanFactory.getBeansWithAnnotation(com.jobq.annotation.Job.class);
        for (Object bean : annotationBeans.values()) {
            if (bean instanceof JobWorker<?>) {
                continue;
            }
            registerAnnotationDrivenJob(registrations, bean);
        }

        this.jobMap = Map.copyOf(registrations);

        Map<String, AtomicBoolean> pollState = new HashMap<>();
        for (String jobType : jobMap.keySet()) {
            pollState.put(jobType, new AtomicBoolean(false));
        }
        this.pollInProgress = pollState;

        log.info("Job Poller initialized on {} with {} registered jobs: {}", nodeId, jobMap.size(), jobMap.keySet());
    }

    @Scheduled(fixedDelayString = "${jobq.background-job-server.poll-interval-in-seconds:15}000")
    public void poll() {
        if (jobMap.isEmpty()) {
            return;
        }
        for (String jobType : jobMap.keySet()) {
            AtomicBoolean inProgress = pollInProgress.get(jobType);
            if (inProgress == null || !inProgress.compareAndSet(false, true)) {
                continue;
            }

            try {
                pollingExecutor.execute(() -> {
                    try {
                        pollForType(jobType);
                    } finally {
                        inProgress.set(false);
                    }
                });
            } catch (RejectedExecutionException saturatedPollingQueue) {
                inProgress.set(false);
                log.debug("Skipping poll dispatch for type {} because polling queue is saturated", jobType);
            }
        }
    }

    private void pollForType(String jobType) {
        RegisteredJob registration = jobMap.get(jobType);
        if (registration == null) {
            return;
        }

        int availableSlots = availableProcessingSlots();
        if (availableSlots <= 0) {
            return;
        }

        // Acquire a bounded batch based on currently available processing capacity.
        List<Job> jobs = transactionTemplate.execute(status -> {
            int batchSize = Math.min(workerCount, availableSlots);
            List<Job> nextJobs = jobRepository.findNextJobsForUpdate(jobType, PageRequest.of(0, batchSize));
            if (nextJobs.isEmpty()) {
                return List.<Job>of();
            }
            OffsetDateTime lockTime = OffsetDateTime.now();
            for (Job j : nextJobs) {
                j.setProcessingStartedAt(lockTime);
                j.setFinishedAt(null);
                j.setFailedAt(null);
                j.setLockedAt(lockTime);
                j.setLockedBy(nodeId);
                j.setUpdatedAt(lockTime);
            }
            jobRepository.saveAll(nextJobs);
            return nextJobs;
        });

        if (jobs == null || jobs.isEmpty()) {
            return;
        }

        for (Job job : jobs) {
            processingExecutor.execute(() -> processJob(job, registration));
        }
    }

    private int availableProcessingSlots() {
        int inFlight = processingExecutor.getActiveCount() + processingExecutor.getQueue().size();
        return workerCount - inFlight;
    }

    private void processJob(Job job, RegisteredJob registration) {
        log.debug("Locked job {} of type {} for processing", job.getId(), job.getType());

        Object payload = null;
        try {
            payload = registration.payloadDeserializer().deserialize(job.getPayload());
            registration.invoker().invoke(job.getId(), payload);
            markCompleted(job, registration);
            invokeOnSuccessSafely(registration, job.getId(), payload);
            log.debug("Successfully completed job {} of type {}", job.getId(), job.getType());
        } catch (Exception e) {
            invokeOnErrorSafely(registration, job.getId(), payload, e);
            if (isExpectedException(registration, e)) {
                log.debug("Job {} of type {} threw expected exception {}. Marking as COMPLETED.",
                        job.getId(), job.getType(), e.getClass().getName());
                markCompleted(job, registration);
                invokeOnSuccessSafely(registration, job.getId(), payload);
                return;
            }

            log.error("Failed to process job {} of type {}", job.getId(), job.getType(), e);
            handleFailure(job, e, registration);
        }
    }

    private void invokeOnErrorSafely(RegisteredJob registration, UUID jobId, Object payload, Exception error) {
        try {
            registration.errorHandler().onError(jobId, payload, error);
        } catch (Exception onErrorFailure) {
            log.error("onError callback failed for job {} of type {}", jobId, registration.type(), onErrorFailure);
        }
    }

    private void invokeOnSuccessSafely(RegisteredJob registration, UUID jobId, Object payload) {
        try {
            registration.successHandler().onSuccess(jobId, payload);
        } catch (Exception onSuccessFailure) {
            log.error("onSuccess/after callback failed for job {} of type {}", jobId, registration.type(),
                    onSuccessFailure);
        }
    }

    private void registerWorkerBasedJob(Map<String, RegisteredJob> registrations, JobWorker<?> worker) {
        String jobType = worker.getJobType();
        Class<?> payloadClass = worker.getPayloadClass();
        com.jobq.annotation.Job annotation = findJobAnnotationOnBean(worker);
        PayloadDeserializer payloadDeserializer = payloadDeserializerFor(payloadClass);

        JobInvoker invoker = (jobId, payload) -> invokeWorker(worker, jobId, payload);
        JobErrorHandler errorHandler = (jobId, payload, exception) -> invokeWorkerOnError(worker, jobId, payload,
                exception);
        JobSuccessHandler successHandler = (jobId, payload) -> invokeWorkerOnSuccess(worker, jobId, payload);
        registerJob(registrations, jobType, payloadDeserializer, annotation, invoker, errorHandler, successHandler,
                "JobWorker bean " + ClassUtils.getUserClass(worker).getName());
    }

    private void registerAnnotationDrivenJob(Map<String, RegisteredJob> registrations, Object bean) {
        com.jobq.annotation.Job annotation = findJobAnnotationOnBean(bean);
        if (annotation == null) {
            return;
        }

        String jobType = annotation.value() == null ? "" : annotation.value().trim();
        if (jobType.isEmpty()) {
            throw new IllegalStateException("@Job value must not be blank on " + ClassUtils.getUserClass(bean).getName());
        }

        Method processMethod = resolveProcessMethod(bean, annotation);
        Class<?> payloadClass = resolvePayloadClass(annotation, processMethod);
        PayloadDeserializer payloadDeserializer = payloadDeserializerFor(payloadClass);
        Method onErrorMethod = resolveOnErrorMethod(bean, payloadClass);
        Method onSuccessMethod = resolveOnSuccessMethod(bean, payloadClass);
        JobInvoker invoker = createProcessInvoker(bean, processMethod);
        JobErrorHandler errorHandler = createOnErrorHandler(bean, onErrorMethod);
        JobSuccessHandler successHandler = createOnSuccessHandler(bean, onSuccessMethod);

        registerJob(registrations, jobType, payloadDeserializer, annotation, invoker, errorHandler, successHandler,
                "@Job bean " + ClassUtils.getUserClass(bean).getName());
    }

    private void registerJob(
            Map<String, RegisteredJob> registrations,
            String jobType,
            PayloadDeserializer payloadDeserializer,
            com.jobq.annotation.Job annotation,
            JobInvoker invoker,
            JobErrorHandler errorHandler,
            JobSuccessHandler successHandler,
            String source) {
        String recurringCron = null;
        CronExpression recurringCronExpression = null;
        if (annotation != null && annotation.cron() != null && !annotation.cron().isBlank()) {
            recurringCron = annotation.cron().trim();
            try {
                recurringCronExpression = CronExpression.parse(recurringCron);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Invalid cron expression '" + recurringCron + "' for job type '" + jobType + "'", e);
            }
        }
        RegisteredJob existing = registrations.putIfAbsent(jobType,
                new RegisteredJob(jobType, payloadDeserializer, annotation, invoker, errorHandler, successHandler,
                        recurringCron,
                        recurringCronExpression));
        if (existing != null) {
            throw new IllegalStateException(
                    "Duplicate job type '" + jobType + "' detected while registering " + source
                            + ". Each job type must be unique.");
        }
    }

    private Method resolveProcessMethod(Object bean, com.jobq.annotation.Job annotation) {
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        List<Method> processMethods = Arrays.stream(ReflectionUtils.getAllDeclaredMethods(targetClass))
                .filter(method -> method.getName().equals("process"))
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .filter(method -> !method.isBridge() && !method.isSynthetic())
                .toList();

        if (processMethods.isEmpty()) {
            throw new IllegalStateException(
                    "@Job bean " + targetClass.getName() + " must declare a non-static process(...) method.");
        }

        Class<?> configuredPayload = annotation.payload();

        if (configuredPayload != Void.class) {
            Method exactTwoArgs = findUniqueMethod(processMethods,
                    method -> hasSignature(method, UUID.class, configuredPayload),
                    targetClass, "(UUID, " + configuredPayload.getSimpleName() + ")");
            if (exactTwoArgs != null) {
                return exactTwoArgs;
            }

            Method assignableTwoArgs = findUniqueMethod(processMethods,
                    method -> method.getParameterCount() == 2
                            && UUID.class.isAssignableFrom(method.getParameterTypes()[0])
                            && method.getParameterTypes()[1].isAssignableFrom(configuredPayload),
                    targetClass, "(UUID, <assignable " + configuredPayload.getSimpleName() + ">)");
            if (assignableTwoArgs != null) {
                return assignableTwoArgs;
            }

            Method exactOneArg = findUniqueMethod(processMethods,
                    method -> hasSignature(method, configuredPayload),
                    targetClass, "(" + configuredPayload.getSimpleName() + ")");
            if (exactOneArg != null) {
                return exactOneArg;
            }

            Method assignableOneArg = findUniqueMethod(processMethods,
                    method -> method.getParameterCount() == 1
                            && !UUID.class.isAssignableFrom(method.getParameterTypes()[0])
                            && method.getParameterTypes()[0].isAssignableFrom(configuredPayload),
                    targetClass, "(<assignable " + configuredPayload.getSimpleName() + ">)");
            if (assignableOneArg != null) {
                return assignableOneArg;
            }

            throw new IllegalStateException(
                    "@Job bean " + targetClass.getName() + " declares payload " + configuredPayload.getName()
                            + " but no matching process(...) method was found.");
        }

        Method inferredTwoArgs = findUniqueMethod(processMethods,
                method -> method.getParameterCount() == 2
                        && UUID.class.isAssignableFrom(method.getParameterTypes()[0]),
                targetClass, "(UUID, Payload)");
        if (inferredTwoArgs != null) {
            return inferredTwoArgs;
        }

        Method inferredOnePayloadArg = findUniqueMethod(processMethods,
                method -> method.getParameterCount() == 1
                        && !UUID.class.isAssignableFrom(method.getParameterTypes()[0]),
                targetClass, "(Payload)");
        if (inferredOnePayloadArg != null) {
            return inferredOnePayloadArg;
        }

        Method uuidOnly = findUniqueMethod(processMethods,
                method -> hasSignature(method, UUID.class),
                targetClass, "(UUID)");
        if (uuidOnly != null) {
            return uuidOnly;
        }

        Method noArgs = findUniqueMethod(processMethods,
                method -> method.getParameterCount() == 0,
                targetClass, "()");
        if (noArgs != null) {
            return noArgs;
        }

        throw new IllegalStateException(
                "@Job bean " + targetClass.getName()
                        + " has no supported process(...) signature. Supported: process(), process(UUID), process(Payload), process(UUID, Payload).");
    }

    private Method findUniqueMethod(
            List<Method> methods,
            java.util.function.Predicate<Method> matcher,
            Class<?> targetClass,
            String signatureDescription) {
        List<Method> matches = methods.stream().filter(matcher).toList();
        if (matches.isEmpty()) {
            return null;
        }
        if (matches.size() > 1) {
            throw new IllegalStateException(
                    "Ambiguous method overloads on " + targetClass.getName() + " for signature "
                            + signatureDescription + ". Keep exactly one matching method.");
        }
        Method selected = matches.get(0);
        ReflectionUtils.makeAccessible(selected);
        return selected;
    }

    private boolean hasSignature(Method method, Class<?>... parameterTypes) {
        return Arrays.equals(method.getParameterTypes(), parameterTypes);
    }

    private Class<?> resolvePayloadClass(com.jobq.annotation.Job annotation, Method processMethod) {
        if (annotation.payload() != Void.class) {
            return annotation.payload();
        }
        Class<?>[] parameterTypes = processMethod.getParameterTypes();
        if (parameterTypes.length == 2) {
            return parameterTypes[1];
        }
        if (parameterTypes.length == 1 && !UUID.class.isAssignableFrom(parameterTypes[0])) {
            return parameterTypes[0];
        }
        return Void.class;
    }

    private PayloadDeserializer payloadDeserializerFor(Class<?> payloadClass) {
        if (payloadClass == Void.class) {
            return rawPayload -> null;
        }

        ObjectReader reader = objectMapper.readerFor(payloadClass);
        return rawPayload -> {
            if (rawPayload == null) {
                return null;
            }
            return reader.readValue(rawPayload);
        };
    }

    private Method resolveOnErrorMethod(Object bean, Class<?> payloadClass) {
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        List<Method> onErrorMethods = Arrays.stream(ReflectionUtils.getAllDeclaredMethods(targetClass))
                .filter(method -> method.getName().equals("onError"))
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .filter(method -> !method.isBridge() && !method.isSynthetic())
                .toList();

        if (onErrorMethods.isEmpty()) {
            return null;
        }

        if (payloadClass != Void.class) {
            Method fullContext = findUniqueMethod(onErrorMethods,
                    method -> hasErrorSignature(method, UUID.class, payloadClass),
                    targetClass, "onError(UUID, Payload, Exception)");
            if (fullContext != null) {
                return fullContext;
            }

            Method payloadAndError = findUniqueMethod(onErrorMethods,
                    method -> hasErrorSignature(method, payloadClass),
                    targetClass, "onError(Payload, Exception)");
            if (payloadAndError != null) {
                return payloadAndError;
            }
        }

        Method idAndError = findUniqueMethod(onErrorMethods,
                method -> hasErrorSignature(method, UUID.class),
                targetClass, "onError(UUID, Exception)");
        if (idAndError != null) {
            return idAndError;
        }

        Method onlyError = findUniqueMethod(onErrorMethods,
                this::hasErrorOnlySignature,
                targetClass, "onError(Exception)");
        if (onlyError != null) {
            return onlyError;
        }

        throw new IllegalStateException(
                "@Job bean " + targetClass.getName()
                        + " has unsupported onError(...) signatures. Supported: onError(Exception), onError(UUID, Exception), onError(Payload, Exception), onError(UUID, Payload, Exception).");
    }

    private Method resolveOnSuccessMethod(Object bean, Class<?> payloadClass) {
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        List<Method> onSuccessMethods = findLifecycleCallbackMethods(targetClass, "onSuccess");
        List<Method> afterMethods = findLifecycleCallbackMethods(targetClass, "after");

        if (!onSuccessMethods.isEmpty() && !afterMethods.isEmpty()) {
            throw new IllegalStateException(
                    "@Job bean " + targetClass.getName()
                            + " must declare either onSuccess(...) or after(...), not both.");
        }

        String callbackName = !onSuccessMethods.isEmpty() ? "onSuccess" : "after";
        List<Method> callbackMethods = !onSuccessMethods.isEmpty() ? onSuccessMethods : afterMethods;
        if (callbackMethods.isEmpty()) {
            return null;
        }

        if (payloadClass != Void.class) {
            Method fullContext = findUniqueMethod(callbackMethods,
                    method -> hasSignature(method, UUID.class, payloadClass),
                    targetClass, callbackName + "(UUID, Payload)");
            if (fullContext != null) {
                return fullContext;
            }

            Method payloadOnly = findUniqueMethod(callbackMethods,
                    method -> hasSignature(method, payloadClass),
                    targetClass, callbackName + "(Payload)");
            if (payloadOnly != null) {
                return payloadOnly;
            }

            Method assignableTwoArgs = findUniqueMethod(callbackMethods,
                    method -> method.getParameterCount() == 2
                            && UUID.class.isAssignableFrom(method.getParameterTypes()[0])
                            && method.getParameterTypes()[1].isAssignableFrom(payloadClass),
                    targetClass, callbackName + "(UUID, <assignable Payload>)");
            if (assignableTwoArgs != null) {
                return assignableTwoArgs;
            }

            Method assignablePayloadOnly = findUniqueMethod(callbackMethods,
                    method -> method.getParameterCount() == 1
                            && !UUID.class.isAssignableFrom(method.getParameterTypes()[0])
                            && method.getParameterTypes()[0].isAssignableFrom(payloadClass),
                    targetClass, callbackName + "(<assignable Payload>)");
            if (assignablePayloadOnly != null) {
                return assignablePayloadOnly;
            }
        }

        Method idOnly = findUniqueMethod(callbackMethods,
                method -> hasSignature(method, UUID.class),
                targetClass, callbackName + "(UUID)");
        if (idOnly != null) {
            return idOnly;
        }

        Method noArgs = findUniqueMethod(callbackMethods,
                method -> method.getParameterCount() == 0,
                targetClass, callbackName + "()");
        if (noArgs != null) {
            return noArgs;
        }

        throw new IllegalStateException(
                "@Job bean " + targetClass.getName()
                        + " has unsupported " + callbackName
                        + "(...) signatures. Supported: " + callbackName
                        + "(), " + callbackName
                        + "(UUID), " + callbackName + "(Payload), " + callbackName + "(UUID, Payload).");
    }

    private List<Method> findLifecycleCallbackMethods(Class<?> targetClass, String methodName) {
        return Arrays.stream(ReflectionUtils.getAllDeclaredMethods(targetClass))
                .filter(method -> method.getName().equals(methodName))
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .filter(method -> !method.isBridge() && !method.isSynthetic())
                .toList();
    }

    private boolean hasErrorSignature(Method method, Class<?>... leadingParameterTypes) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != leadingParameterTypes.length + 1) {
            return false;
        }
        for (int i = 0; i < leadingParameterTypes.length; i++) {
            if (!parameterTypes[i].equals(leadingParameterTypes[i])) {
                return false;
            }
        }
        return acceptsSupportedOnErrorExceptionType(parameterTypes[parameterTypes.length - 1]);
    }

    private boolean hasErrorOnlySignature(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        return parameterTypes.length == 1 && acceptsSupportedOnErrorExceptionType(parameterTypes[0]);
    }

    private boolean acceptsSupportedOnErrorExceptionType(Class<?> parameterType) {
        return parameterType == Exception.class || parameterType == Throwable.class;
    }

    private JobInvoker createProcessInvoker(Object bean, Method processMethod) {
        int parameterCount = processMethod.getParameterCount();
        if (parameterCount == 0) {
            return (jobId, payload) -> invokeReflectively(bean, processMethod);
        }
        if (parameterCount == 1) {
            Class<?> singleParameter = processMethod.getParameterTypes()[0];
            if (UUID.class.isAssignableFrom(singleParameter)) {
                return (jobId, payload) -> invokeReflectively(bean, processMethod, jobId);
            }
            return (jobId, payload) -> invokeReflectively(bean, processMethod, payload);
        }
        return (jobId, payload) -> invokeReflectively(bean, processMethod, jobId, payload);
    }

    private JobErrorHandler createOnErrorHandler(Object bean, Method onErrorMethod) {
        if (onErrorMethod == null) {
            return (jobId, payload, exception) -> {
            };
        }

        int parameterCount = onErrorMethod.getParameterCount();
        if (parameterCount == 1) {
            return (jobId, payload, exception) -> invokeReflectively(bean, onErrorMethod, exception);
        }
        if (parameterCount == 2) {
            if (UUID.class.isAssignableFrom(onErrorMethod.getParameterTypes()[0])) {
                return (jobId, payload, exception) -> invokeReflectively(bean, onErrorMethod, jobId, exception);
            }
            return (jobId, payload, exception) -> invokeReflectively(bean, onErrorMethod, payload, exception);
        }
        return (jobId, payload, exception) -> invokeReflectively(bean, onErrorMethod, jobId, payload, exception);
    }

    private JobSuccessHandler createOnSuccessHandler(Object bean, Method onSuccessMethod) {
        if (onSuccessMethod == null) {
            return (jobId, payload) -> {
            };
        }

        int parameterCount = onSuccessMethod.getParameterCount();
        if (parameterCount == 0) {
            return (jobId, payload) -> invokeReflectively(bean, onSuccessMethod);
        }
        if (parameterCount == 1) {
            if (UUID.class.isAssignableFrom(onSuccessMethod.getParameterTypes()[0])) {
                return (jobId, payload) -> invokeReflectively(bean, onSuccessMethod, jobId);
            }
            return (jobId, payload) -> invokeReflectively(bean, onSuccessMethod, payload);
        }
        return (jobId, payload) -> invokeReflectively(bean, onSuccessMethod, jobId, payload);
    }

    private void invokeReflectively(Object bean, Method method, Object... args) throws Exception {
        try {
            method.invoke(bean, args);
        } catch (InvocationTargetException invocationTargetException) {
            Throwable target = invocationTargetException.getTargetException();
            if (target instanceof Exception ex) {
                throw ex;
            }
            if (target instanceof Error error) {
                throw error;
            }
            throw new RuntimeException(target);
        }
    }

    private void invokeWorker(JobWorker<?> worker, UUID jobId, Object payload) throws Exception {
        @SuppressWarnings("unchecked")
        JobWorker<Object> castWorker = (JobWorker<Object>) worker;
        castWorker.process(jobId, payload);
    }

    private void invokeWorkerOnError(JobWorker<?> worker, UUID jobId, Object payload, Exception exception) throws Exception {
        @SuppressWarnings("unchecked")
        JobWorker<Object> castWorker = (JobWorker<Object>) worker;
        castWorker.onError(jobId, payload, exception);
    }

    private void invokeWorkerOnSuccess(JobWorker<?> worker, UUID jobId, Object payload) throws Exception {
        @SuppressWarnings("unchecked")
        JobWorker<Object> castWorker = (JobWorker<Object>) worker;
        castWorker.after(jobId, payload);
    }

    private com.jobq.annotation.Job findJobAnnotationOnBean(Object bean) {
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        return AnnotationUtils.findAnnotation(targetClass, com.jobq.annotation.Job.class);
    }

    private void markCompleted(Job jobSnapshot, RegisteredJob registration) {
        OffsetDateTime now = OffsetDateTime.now();
        Integer updated = transactionTemplate
                .execute(status -> jobRepository.markCompleted(jobSnapshot.getId(), now, nodeId));
        if (toAffectedRows(updated) > 0) {
            scheduleNextRecurringExecutionIfNeeded(jobSnapshot, registration);
            return;
        }

        log.debug("Falling back to entity completion update for job {}", jobSnapshot.getId());
        transactionTemplate.executeWithoutResult(status -> jobRepository.findById(jobSnapshot.getId()).ifPresent(job -> {
            if (!isMutableProcessingJob(job)) {
                log.debug("Skipping completion fallback for job {} due lifecycle/lock mismatch", jobSnapshot.getId());
                return;
            }
            if (job.getProcessingStartedAt() == null) {
                job.setProcessingStartedAt(now);
            }
            job.setFinishedAt(now);
            job.setFailedAt(null);
            job.setErrorMessage(null);
            job.setLockedAt(null);
            job.setLockedBy(null);
            job.setUpdatedAt(now);
            jobRepository.save(job);
            scheduleNextRecurringExecutionIfNeeded(job, registration);
        }));
    }

    private void scheduleNextRecurringExecutionIfNeeded(Job job, RegisteredJob registration) {
        if (job.getCron() == null || job.getCron().isBlank()) {
            return;
        }
        if (registration.recurringCronExpression() == null || registration.recurringCron() == null) {
            return;
        }

        try {
            OffsetDateTime nextRun = registration.recurringCronExpression().next(OffsetDateTime.now());
            if (nextRun == null) {
                return;
            }

            Job nextJob = new Job(UUID.randomUUID(), job.getType(), job.getPayload(),
                    job.getMaxRetries(), job.getPriority(), job.getGroupId(), recurringReplaceKey(registration.recurringCron()));
            nextJob.setCron(registration.recurringCron());
            nextJob.setRunAt(nextRun);
            try {
                jobRepository.save(nextJob);
                log.info("Scheduled next execution of recurring job {} at {}", job.getType(), nextRun);
            } catch (DataIntegrityViolationException duplicateSchedule) {
                log.debug("Skipped duplicate recurring schedule for type {} and cron '{}'", job.getType(),
                        registration.recurringCron());
            }
        } catch (Exception cronEx) {
            log.error("Failed to reschedule recurring job {} with cron '{}'",
                    job.getType(), registration.recurringCron(), cronEx);
        }
    }

    private String recurringReplaceKey(String cron) {
        return "__jobq_recurring__:" + cron;
    }

    private void handleFailure(Job jobSnapshot, Exception exception, RegisteredJob registration) {
        OffsetDateTime now = OffsetDateTime.now();
        int currentRetryCount = jobSnapshot.getRetryCount();
        int nextRetryCount = currentRetryCount + 1;
        String errorMessage = exception.getMessage();

        if (nextRetryCount > jobSnapshot.getMaxRetries()) {
            Integer updated = transactionTemplate.execute(status -> jobRepository.markFailedTerminal(
                    jobSnapshot.getId(),
                    currentRetryCount,
                    nextRetryCount,
                    errorMessage,
                    now,
                    nodeId));
            if (toAffectedRows(updated) > 0) {
                return;
            }
        } else {
            RetryDecision retryDecision = computeRetryDecision(jobSnapshot, registration, nextRetryCount, now);
            Integer updated = transactionTemplate.execute(status -> jobRepository.markForRetry(
                    jobSnapshot.getId(),
                    currentRetryCount,
                    nextRetryCount,
                    errorMessage,
                    now,
                    retryDecision.nextRunAt(),
                    retryDecision.nextPriority(),
                    nodeId));
            if (toAffectedRows(updated) > 0) {
                return;
            }
        }

        log.debug("Falling back to entity failure update for job {}", jobSnapshot.getId());
        fallbackFailureUpdate(jobSnapshot.getId(), exception, registration);
    }

    private void fallbackFailureUpdate(UUID jobId, Exception exception, RegisteredJob registration) {
        transactionTemplate.executeWithoutResult(status -> jobRepository.findById(jobId).ifPresent(job -> {
            if (!isMutableProcessingJob(job)) {
                log.debug("Skipping failure fallback for job {} due lifecycle/lock mismatch", jobId);
                return;
            }
            OffsetDateTime now = OffsetDateTime.now();
            job.setErrorMessage(exception.getMessage());
            job.incrementRetryCount();
            job.setUpdatedAt(now);

            if (job.getRetryCount() > job.getMaxRetries()) {
                if (job.getProcessingStartedAt() == null) {
                    job.setProcessingStartedAt(now);
                }
                job.setFailedAt(now);
                job.setFinishedAt(null);
                job.setLockedAt(null);
                job.setLockedBy(null);
            } else {
                RetryDecision retryDecision = computeRetryDecision(job, registration, job.getRetryCount(), now);
                job.setProcessingStartedAt(null);
                job.setFinishedAt(null);
                job.setFailedAt(null);
                job.setLockedAt(null);
                job.setLockedBy(null);
                job.setRunAt(retryDecision.nextRunAt());
                job.setPriority(retryDecision.nextPriority());
            }
            jobRepository.save(job);
        }));
    }

    private RetryDecision computeRetryDecision(Job job, RegisteredJob registration, int retryCount, OffsetDateTime now) {
        com.jobq.annotation.Job jobAnnotation = registration.annotation();
        int nextPriority = job.getPriority();
        OffsetDateTime nextRunAt;

        if (jobAnnotation != null) {
            long delayMs = (long) (jobAnnotation.initialBackoffMs()
                    * Math.pow(jobAnnotation.backoffMultiplier(), retryCount - 1));
            nextRunAt = now.plusNanos(delayMs * 1_000_000);

            if (jobAnnotation.retryPriority() == com.jobq.annotation.Job.RetryPriority.LOWER_ON_RETRY) {
                nextPriority = job.getPriority() - 1;
            } else if (jobAnnotation.retryPriority() == com.jobq.annotation.Job.RetryPriority.HIGHER_ON_RETRY) {
                nextPriority = job.getPriority() + 1;
            }
            return new RetryDecision(nextRunAt, nextPriority);
        }

        long delaySeconds = (long) Math.pow(properties.getJobs().getRetryBackOffTimeSeed(),
                Math.max(1, retryCount));
        nextRunAt = now.plusSeconds(delaySeconds);
        return new RetryDecision(nextRunAt, nextPriority);
    }

    private boolean isExpectedException(RegisteredJob registration, Exception exception) {
        com.jobq.annotation.Job jobAnnotation = registration.annotation();
        if (jobAnnotation == null || jobAnnotation.expectedExceptions().length == 0) {
            return false;
        }

        Throwable current = exception;
        while (current != null) {
            for (Class<? extends Throwable> expected : jobAnnotation.expectedExceptions()) {
                if (expected.isAssignableFrom(current.getClass())) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private int toAffectedRows(Integer updatedRows) {
        return updatedRows == null ? 0 : updatedRows;
    }

    private boolean isMutableProcessingJob(Job job) {
        return job.getProcessingStartedAt() != null
                && job.getFinishedAt() == null
                && job.getFailedAt() == null
                && job.getLockedAt() != null
                && nodeId.equals(job.getLockedBy());
    }

    private record RetryDecision(OffsetDateTime nextRunAt, int nextPriority) {
    }

    @FunctionalInterface
    private interface JobInvoker {
        void invoke(UUID jobId, Object payload) throws Exception;
    }

    @FunctionalInterface
    private interface PayloadDeserializer {
        Object deserialize(JsonNode rawPayload) throws Exception;
    }

    private record RegisteredJob(
            String type,
            PayloadDeserializer payloadDeserializer,
            com.jobq.annotation.Job annotation,
            JobInvoker invoker,
            JobErrorHandler errorHandler,
            JobSuccessHandler successHandler,
            String recurringCron,
            CronExpression recurringCronExpression) {
    }

    @FunctionalInterface
    private interface JobErrorHandler {
        void onError(UUID jobId, Object payload, Exception exception) throws Exception;
    }

    @FunctionalInterface
    private interface JobSuccessHandler {
        void onSuccess(UUID jobId, Object payload) throws Exception;
    }

    @PreDestroy
    void shutdownExecutor() {
        pollingExecutor.shutdown();
        processingExecutor.shutdown();
    }
}
