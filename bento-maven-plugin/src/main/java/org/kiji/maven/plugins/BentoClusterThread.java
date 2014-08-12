/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.maven.plugins;

import org.apache.maven.plugin.logging.Log;

/**
 * A Thread to run a Bento cluster.
 */
public class BentoClusterThread extends Thread implements MavenLoggable {
  /** The maven log. */
  private final Log mLog;

  /** The local Bento cluster. */
  private final BentoCluster mBentoCluster;

  /** Monitor to track when the Bento cluster is started and when it is requested to stop. */
  private final Object mMonitor = new Object();

  /** Whether the cluster is started and ready. */
  private volatile boolean mIsClusterReady;

  /** Whether the thread has been asked to stop. */
  private volatile boolean mIsStopRequested;

  /**
   * Creates a new <code>BentoClusterThread</code> instance.
   *
   * @param log The maven log.
   * @param bentoCluster The Bento cluster to run.
   */
  public BentoClusterThread(Log log, BentoCluster bentoCluster) {
    mLog = log;
    mBentoCluster = bentoCluster;
    mIsClusterReady = false;
    mIsStopRequested = false;
  }

  /**
   * Determine whether the Bento cluster is up and running.
   *
   * @return Whether the cluster has completed startup.
   */
  public boolean isClusterReady() {
    return mIsClusterReady;
  }

  /**
   * Stops the Bento cluster gracefully.  When it is fully shut down, the thread will exit.
   */
  public void stopClusterGracefully() {
    // Notify the run method to stop waiting and gracefully shutdown.
    synchronized (mMonitor) {
      mIsStopRequested = true;
      mMonitor.notifyAll();
    }
  }

  @Override
  public Log getLog() {
    return mLog;
  }

  @Override
  public void run() {
    getLog().info("Starting up Bento cluster...");
    boolean startedSuccessfully;
    try {
      mBentoCluster.startup();
      startedSuccessfully = true;
    } catch (Exception e) {
      getLog().error("Error starting Bento cluster.", e);
      startedSuccessfully = false;
    }

    if (startedSuccessfully) {
      getLog().info("Bento cluster started.");
      mIsClusterReady = true;
      yield();

      // Twiddle our thumbs until notified to stop.
      synchronized (mMonitor) {
        try {
          while (!mIsStopRequested) {
            mMonitor.wait();
          }
        } catch (InterruptedException e) {
          getLog().debug("Main thread interrupted while waiting for cluster to stop.");
          // Interrupt this thread.
          Thread.currentThread().interrupt();
        }
      }
    }

    getLog().info("Starting graceful shutdown of the Bento cluster...");
    try {
      mBentoCluster.shutdown();
    } catch (Exception e) {
      getLog().error("Unable to stop the Bento cluster.", e);
      return;
    }
    getLog().info("Bento shut down.");
  }
}
