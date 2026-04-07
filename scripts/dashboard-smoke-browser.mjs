import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { chromium, firefox, webkit } from 'playwright';

const outputDir = process.env.JOBQ_SMOKE_OUTPUT_DIR;
const dashboardUrl = process.env.JOBQ_SMOKE_URL;
const browserName = process.env.JOBQ_SMOKE_BROWSER || 'chromium';
const headless = process.env.JOBQ_SMOKE_HEADLESS !== '0';

if (!outputDir) {
  throw new Error('JOBQ_SMOKE_OUTPUT_DIR is required');
}
if (!dashboardUrl) {
  throw new Error('JOBQ_SMOKE_URL is required');
}

const browsers = { chromium, firefox, webkit };
const browserType = browsers[browserName];
if (!browserType) {
  throw new Error(`Unsupported browser '${browserName}'. Expected one of: ${Object.keys(browsers).join(', ')}`);
}

const consoleMessages = [];
const pageErrors = [];
const requestFailures = [];
const responseErrors = [];

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function assertContains(text, needle, label) {
  assert(text.includes(needle), `${label}: expected to find '${needle}'`);
}

function assertNotContains(text, needle, label) {
  assert(!text.includes(needle), `${label}: unexpected '${needle}'`);
}

async function writeArtifact(fileName, content) {
  await fs.writeFile(path.join(outputDir, fileName), content, 'utf8');
}

async function main() {
  await fs.mkdir(outputDir, { recursive: true });

  const browser = await browserType.launch({ headless });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 2200 },
    ignoreHTTPSErrors: true,
  });

  await context.tracing.start({ screenshots: true, snapshots: true });

  const page = await context.newPage();
  page.setDefaultTimeout(20_000);

  page.on('console', (message) => {
    consoleMessages.push(`[${message.type()}] ${message.text()}`);
  });
  page.on('pageerror', (error) => {
    pageErrors.push(error.stack || String(error));
  });
  page.on('requestfailed', (request) => {
    const failure = request.failure();
    requestFailures.push(`${request.method()} ${request.url()} ${failure?.errorText || 'request failed'}`);
  });
  page.on('response', (response) => {
    if (response.status() >= 400 && !response.url().endsWith('/favicon.ico')) {
      responseErrors.push(`${response.status()} ${response.request().method()} ${response.url()}`);
    }
  });

  const finalizeArtifacts = async () => {
    await writeArtifact('console-messages.txt', `${consoleMessages.join('\n')}\n`);
    await writeArtifact('page-errors.txt', `${pageErrors.join('\n')}\n`);
    await writeArtifact('request-failures.txt', `${requestFailures.join('\n')}\n`);
    await writeArtifact('response-errors.txt', `${responseErrors.join('\n')}\n`);
  };

  try {
    await page.goto(dashboardUrl, { waitUntil: 'domcontentloaded' });

    await page.waitForFunction(() => {
      const statsText = document.querySelector('#stats-container')?.textContent || '';
      const jobsText = document.querySelector('#jobs-container')?.textContent || '';
      const queueText = document.querySelector('#jobq-queues-panel')?.textContent || '';
      return statsText.includes('Total Jobs')
        && jobsText.includes('CANCELLED')
        && queueText.includes('Show all');
    });

    const statsText = (await page.locator('#stats-container').textContent()) || '';
    await writeArtifact('stats.txt', `${statsText}\n`);
    assertContains(statsText, 'Total Jobs', 'Dashboard stats');
    assertContains(statsText, '7', 'Dashboard stats total');
    assertContains(statsText, 'Cancelled', 'Dashboard stats cancelled label');

    const jobsText = (await page.locator('#jobs-container').textContent()) || '';
    await writeArtifact('jobs.txt', `${jobsText}\n`);
    assertContains(jobsText, 'Run now', 'Jobs table delayed-action button');
    assertContains(jobsText, 'Retry', 'Jobs table failed-action button');
    assertContains(jobsText, 'Rerun', 'Jobs table rerun-action button');
    assertContains(jobsText, 'CANCELLED', 'Jobs table cancelled row');
    assertContains(jobsText, 'com.example.jobs.CleanupJob', 'Jobs table cleanup row');

    const queueText = (await page.locator('#jobq-queues-panel').textContent()) || '';
    await writeArtifact('queues.txt', `${queueText}\n`);
    assertContains(queueText, 'Queues with active work, failures, controls, or cron schedules', 'Queue panel description');
    assertContains(queueText, '5 shown', 'Queue panel relevant count');
    assertContains(queueText, 'com.example.jobs.ImportJob', 'Queue panel import queue');
    assertContains(queueText, 'com.example.jobs.ReportJob', 'Queue panel report queue');
    assertContains(queueText, 'RECURRING_JOB', 'Queue panel recurring queue');
    assertContains(queueText, 'com.example.jobs.DelayedReminderJob', 'Queue panel delayed queue');
    assertContains(queueText, 'com.example.jobs.SyncJob', 'Queue panel sync queue');
    assertNotContains(queueText, 'com.example.jobs.CleanupJob', 'Queue panel should hide cancelled-only queue');
    assertNotContains(queueText, 'com.example.jobs.CompletedEmailJob', 'Queue panel should hide completed-only queue');

    const searchInput = page.locator('input[name="queryInput"]');
    await searchInput.fill('com.example.jobs.CleanupJob');
    await page.waitForFunction(() => {
      const text = document.querySelector('#jobs-container')?.textContent || '';
      return text.includes('com.example.jobs.CleanupJob')
        && text.includes('CANCELLED')
        && !text.includes('com.example.jobs.ReportJob');
    });
    const filteredJobsText = (await page.locator('#jobs-container').textContent()) || '';
    await writeArtifact('filtered-jobs.txt', `${filteredJobsText}\n`);
    assertContains(filteredJobsText, 'com.example.jobs.CleanupJob', 'Filtered jobs table cleanup row');
    assertContains(filteredJobsText, 'CANCELLED', 'Filtered jobs table cancelled status');
    assertNotContains(filteredJobsText, 'com.example.jobs.ReportJob', 'Filtered jobs table should exclude report row');

    await page.locator('button[onclick="jobqClearFilters()"]').click();
    await page.waitForFunction(() => {
      const text = document.querySelector('#jobs-container')?.textContent || '';
      return text.includes('com.example.jobs.ReportJob') && text.includes('com.example.jobs.CleanupJob');
    });

    await page.locator('#jobq-ops-tab-metrics').click();
    await page.waitForFunction(() => {
      const text = document.querySelector('#jobq-metrics-panel')?.textContent || '';
      return text.includes('Failure rate') && text.includes('Queue p50');
    });
    const metricsText = (await page.locator('#jobq-metrics-panel').textContent()) || '';
    await writeArtifact('metrics.txt', `${metricsText}\n`);
    assertContains(metricsText, 'Failure rate', 'Metrics panel failure card');
    assertContains(metricsText, 'Queue p50', 'Metrics panel queue latency');
    assertContains(metricsText, 'Runtime p50', 'Metrics panel runtime latency');

    await page.locator('#jobq-ops-tab-queues').click();
    await page.waitForFunction(() => {
      return (document.querySelector('#jobq-queues-panel')?.textContent || '').includes('Show all');
    });
    await page.locator('button[onclick="jobqToggleQueueScope()"]').click();
    await page.waitForFunction(() => {
      const text = document.querySelector('#jobq-queues-panel')?.textContent || '';
      return text.includes('All registered job types') && text.includes('com.example.jobs.CleanupJob');
    });
    const allQueueText = (await page.locator('#jobq-queues-panel').textContent()) || '';
    await writeArtifact('all-queues.txt', `${allQueueText}\n`);
    assertContains(allQueueText, 'All registered job types', 'Queue panel expanded scope');
    assertContains(allQueueText, 'com.example.jobs.CleanupJob', 'Queue panel all scope cleanup queue');
    assertContains(allQueueText, 'com.example.jobs.CompletedEmailJob', 'Queue panel all scope completed queue');

    await page.locator('#jobs-container tr').filter({ hasText: 'com.example.jobs.ReportJob' }).first().click();
    await page.waitForFunction(() => {
      return (document.querySelector('#modal-container')?.textContent || '').includes('Downstream timeout while generating report');
    });
    const modalText = (await page.locator('#modal-container').textContent()) || '';
    await writeArtifact('modal.txt', `${modalText}\n`);
    assertContains(modalText, 'Downstream timeout while generating report', 'Job details modal error text');
    assertContains(modalText, 'com.example.jobs.ReportJob', 'Job details modal type');

    const consoleText = consoleMessages.join('\n');
    assertNotContains(consoleText, 'swapError', 'Browser console');
    assertNotContains(consoleText, 'ReferenceError', 'Browser console');
    assertNotContains(consoleText, 'TypeError', 'Browser console');
    assert(pageErrors.length === 0, `Unexpected page errors: ${pageErrors.join(' | ')}`);
    assert(requestFailures.length === 0, `Unexpected request failures: ${requestFailures.join(' | ')}`);
    assert(responseErrors.length === 0, `Unexpected HTTP error responses: ${responseErrors.join(' | ')}`);

    await page.screenshot({ path: path.join(outputDir, 'final.png'), fullPage: true });
    await writeArtifact('page-title.txt', `${await page.title()}\n`);
    await finalizeArtifacts();
  } catch (error) {
    await finalizeArtifacts();
    try {
      await page.screenshot({ path: path.join(outputDir, 'failure.png'), fullPage: true });
      await writeArtifact('page.html', await page.content());
    } catch {
      // Ignore secondary artifact failures.
    }
    throw error;
  } finally {
    await context.tracing.stop({ path: path.join(outputDir, 'trace.zip') });
    await context.close();
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error.stack || String(error));
  process.exitCode = 1;
});
