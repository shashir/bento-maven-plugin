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

import java.io.File;
import java.io.IOException;

import org.apache.maven.plugin.logging.Log;

/**
 * A singleton instance of a Bento cluster.
 */
public enum BentoClusterSingleton {
  /** The singleton instance. */
  INSTANCE;

  /** The thread that runs the Bento cluster. */
  private BentoClusterThread mThread;

  /** The Bento cluster being run. */
  private BentoCluster mCluster;

  /**
   * Starts the Bento cluster and blocks until it is ready.
   *
   * @param log The maven log.
   * @param bentoDirPath path to the bento-cluster environment installation.
   * @param bentoName name of the Bento cluster container.
   * @throws java.io.IOException If there is an error.
   */
  public void startAndWaitUntilReady(
      Log log,
      File bentoDirPath,
      String bentoName) throws IOException {
    mCluster = new BentoCluster(log, bentoDirPath, bentoName);
    mThread = new BentoClusterThread(log, mCluster);

    log.info("Starting new thread...");
    mThread.start();

    // Wait for the cluster to be ready.
    log.info("Waiting for cluster to be ready...");
    while (!mThread.isClusterReady() && mThread.isAlive()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        log.info("Interrupted...");
        Thread.currentThread().interrupt();
      }
      log.debug("Still waiting...");
    }
    log.info("Finished waiting for Bento cluster thread.");
  }

  /**
   * Gracefully shutdown the Bento cluster container.
   *
   * @param log The maven log.
   */
  public void stop(Log log) {
    if (null == mCluster) {
      log.error("Attempted to stop a cluster, but no cluster was ever started in this process.");
      return;
    }

    log.info("Stopping the Bento cluster thread...");
    mThread.stopClusterGracefully();
    while (mThread.isAlive()) {
      try {
        mThread.join();
      } catch (InterruptedException e) {
        log.info("Bento cluster thread interrupted.");
      }
    }
    log.info("Bento cluster thread stopped.");
  }
}
