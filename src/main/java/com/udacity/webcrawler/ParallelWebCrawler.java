package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final List<Pattern> ignoredUrls;
  private final int maxDepth;
  private final PageParserFactory parserFactory;

  @Inject
  ParallelWebCrawler(
      Clock clock,
      @Timeout Duration timeout,
      @PopularWordCount int popularWordCount,
      @TargetParallelism int threadCount,
      @IgnoredUrls List<Pattern> ignoredUrls,
      @MaxDepth int maxDepth,
      PageParserFactory parserFactory) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.ignoredUrls = ignoredUrls;
    this.maxDepth = maxDepth;
    this.parserFactory = parserFactory;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);
    ConcurrentMap<String, Integer> wordCounts = new ConcurrentHashMap<>();
    ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();
    for (String url : startingUrls) {
      pool.invoke(new RecursiveInternalCrawler(url, deadline, maxDepth, wordCounts, visitedUrls));
    }

    CrawlResult result = new CrawlResult.Builder().setWordCounts(WordCounts.sort(wordCounts, popularWordCount)).setUrlsVisited(visitedUrls.size()).build();

    return result;
  }

  public class RecursiveInternalCrawler extends RecursiveAction  {
  private String url;
  private Instant deadline;
  private int maxDepth;
  private ConcurrentMap<String, Integer> wordCounts;
  private ConcurrentSkipListSet<String> crawledUrls;
  private final Lock crawledUrlsLock = new ReentrantLock();
  private final Lock wordCountsLock = new ReentrantLock();

    public RecursiveInternalCrawler(
        String url, 
        Instant deadline, 
        int maxDepth, 
        ConcurrentMap<String, Integer> wordCounts, 
        ConcurrentSkipListSet<String> crawledUrls) {
      this.url = url;
      this.deadline = deadline;
      this.maxDepth = maxDepth;
      this.wordCounts = wordCounts;
      this.crawledUrls = crawledUrls;
    }

    @Override
    protected void compute() {
      // Check depth and timeout
      if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
        return;
      }

      // Ignore crawling if url is in the list of ignored urls
      for (Pattern ignoreUrlPattern : ignoredUrls) {
        if (ignoreUrlPattern.matcher(url).matches()) {
          return;
        }
      }
      
      // Do not crawl again this url: using ConcurrentSkipListSet -> Thread-safe
      crawledUrlsLock.lock();
      if (crawledUrls.contains(url)) {
        return;
      }
      crawledUrls.add(url);
      crawledUrlsLock.unlock();

      PageParser.Result result = parserFactory.get(url).parse();

      // Count words
      for (ConcurrentMap.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
        // wordCounts.compute(e.getKey(), (k, v) -> (v == null) ? e.getValue() : e.getValue() + v);
        wordCountsLock.lock();
        if (wordCounts.containsKey(e.getKey())) {
          Integer currentValue = wordCounts.get(e.getKey());
          wordCounts.put(e.getKey(), currentValue + e.getValue());
        } else {
            wordCounts.put(e.getKey(), e.getValue());
        }
        wordCountsLock.unlock();
      }

      // Crawl internal links
      List<RecursiveInternalCrawler> internalCrawlingTasks = new ArrayList<>();
      for (String link : result.getLinks()) {
        internalCrawlingTasks.add(new RecursiveInternalCrawler(link, deadline, maxDepth -1, wordCounts, crawledUrls));
      }
      invokeAll(internalCrawlingTasks);
    }
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}
