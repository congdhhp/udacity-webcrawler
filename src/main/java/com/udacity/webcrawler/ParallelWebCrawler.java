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
    ConcurrentMap<String, Integer> counts = new ConcurrentHashMap<>();
    ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();
    for (String url : startingUrls) {
      pool.invoke(new RecursiveInternalCrawler(url, deadline, maxDepth, counts, visitedUrls));
    }

    return counts.isEmpty() ? (new CrawlResult.Builder().setWordCounts(counts).setUrlsVisited(visitedUrls.size()).build()) : 
      (new CrawlResult.Builder().setWordCounts(WordCounts.sort(counts, popularWordCount)).setUrlsVisited(visitedUrls.size()).build());
  }

  public class RecursiveInternalCrawler extends RecursiveAction  {
  private String url;
  private Instant deadline;
  private int maxDepth;
  private ConcurrentMap<String, Integer> counts;
  private ConcurrentSkipListSet<String> visitedUrls;

    public RecursiveInternalCrawler(
        String url, 
        Instant deadline, 
        int maxDepth, 
        ConcurrentMap<String, Integer> counts, 
        ConcurrentSkipListSet<String> visitedUrls) {
      this.url = url;
      this.deadline = deadline;
      this.maxDepth = maxDepth;
      this.counts = counts;
      this.visitedUrls = visitedUrls;
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

      // Do not crawl again with visited urls
      if (visitedUrls.contains(url)) {
        return;
      }
      visitedUrls.add(url);
      PageParser.Result result = parserFactory.get(url).parse();

      // Count words
      for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
        if (counts.containsKey(e.getKey())) {
          counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
        } else {
          counts.put(e.getKey(), e.getValue());
        }
      }

      // Crawl internal links
      List<RecursiveInternalCrawler> internalCrawlingTasks = new ArrayList<>();
      for (String link : result.getLinks()) {
        internalCrawlingTasks.add(new RecursiveInternalCrawler(link, deadline, maxDepth -1, counts, visitedUrls));
      }
      invokeAll(internalCrawlingTasks);
    }
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}
