/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path

import org.apache.spark.{Logging, Partition, SerializableWritable, SparkException}
import org.apache.spark.scheduler.{ResultTask, ShuffleMapTask}

/**
 * Enumeration to manage state transitions of an RDD through checkpointing
 * [ Initialized --> marked for checkpointing --> checkpointing in progress --> checkpointed ]
 */
private[spark] object CheckpointState extends Enumeration {
  type CheckpointState = Value
  val Initialized, MarkedForCheckpoint, CheckpointingInProgress, Checkpointed = Value
}

/**
 * This class contains all the information related to RDD checkpointing. Each instance of this
 * class is associated with a RDD. It manages process of checkpointing of the associated RDD,
 * as well as, manages the post-checkpoint state by providing the updated partitions,
 * iterator and preferred locations of the checkpointed RDD.
 */
private[spark] class RDDCheckpointData[T: ClassTag](@transient rdd: RDD[T])
  extends Logging with Serializable {

  import CheckpointState._

  // The checkpoint state of the associated RDD.
  var cpState = Initialized

  // The file to which the associated RDD has been checkpointed to
  @transient var cpFile: Option[String] = None

  // The CheckpointRDD created from the checkpoint file, that is, the new parent of the associated RDD.
  var cpRDD: Option[RDD[T]] = None

  // Mark the RDD for checkpointing
  def markForCheckpoint() {
    RDDCheckpointData.synchronized {
      if (cpState == Initialized) cpState = MarkedForCheckpoint
    }
  }

  // Is the RDD already checkpointed
  def isCheckpointed: Boolean = {
    RDDCheckpointData.synchronized { cpState == Checkpointed }
  }

  // Get the file to which this RDD was checkpointed to as an Option
  def getCheckpointFile: Option[String] = {
    RDDCheckpointData.synchronized { cpFile }
  }


/**
  * Similar to doCheckpoint below, except it acts only on 1 partition
  * Called from the scheduler via RDD. Who makes the decision to checkpoint or not? 
  * Scheduler, TaskSetManager, RDD, RDDCheckpointData ?
  * RDD: Has access to graph. RDDs already marked for checkpointing. 
  * Need: Partition size, estmimated recompute cost.
  * 
  */
  def CheckpointPartitionActual (partitionId: Int) :Int = {
    //Write the partition here. Partition ID is a Task parameter.
    // Create the output path for the checkpoint
    val path = new Path(rdd.context.checkpointDir.get, "rdd-" + rdd.id)
    val fs = path.getFileSystem(rdd.context.hadoopConfiguration)
    if (!fs.mkdirs(path)) {
      throw new SparkException("Failed to create checkpoint path " + path)
    }

    // Save to file, and reload it as an RDD
    val broadcastedConf = rdd.context.broadcast(
      new SerializableWritable(rdd.context.hadoopConfiguration))
    /* runJob(rdd, iterator => something, result _, partition list, false) underscore=partially applied function*/
    val partitionToCkpt = List(partitionId)
    val start_time:Long = System.currentTimeMillis()
    rdd.context.runJob(rdd, CheckpointRDD.writeToFile[T](path.toString, broadcastedConf) _, partitionToCkpt, false)
    //Who catches the failure here? What if the partition write fails?
    //Right place to add to the partitions already checkpointed list.
    //if all partitions done, then do the dependency pruning here?
    val end_time:Long =  System.currentTimeMillis()

    val pdone = rdd.addToSavedPartitions(partitionId)
    logInfo("Checkpointed Partition " + rdd.id + ":" +partitionId+ "@" + pdone + "/" + " to " +path)

    if(pdone == rdd.partitions.size) { 
      //logInfo("All partitions done for "+ rdd.id + "/"+rdd.total_num_parts)

      val newRDD = new CheckpointRDD[T](rdd.context, path.toString)
      if (newRDD.partitions.size != rdd.partitions.size) {
        throw new SparkException(
          "Checkpoint RDD " + newRDD + "(" + newRDD.partitions.size + ") has different " +
            "number of partitions than original RDD " + rdd + "(" + rdd.partitions.size + ")")
      }
      RDDCheckpointData.synchronized {
        cpFile = Some(path.toString)
        cpRDD = Some(newRDD)
        rdd.markCheckpointed(newRDD)   // Update the RDD's dependencies and partitions
        cpState = Checkpointed
      }
      logInfo("Finished checkpointing RDD " + rdd.id + " to " + path + ", new parent is RDD " + newRDD.id)
    }
    var td = (end_time - start_time)/1000
    var ti = td.toInt 
    return ti
  }

  // Do the checkpointing of the RDD. Called after the first job using that RDD is over.
  def doCheckpoint() {
    // If it is marked for checkpointing AND checkpointing is not already in progress,
    // then set it to be in progress, else return
    // doCheckpoint is called on *every* rdd, so check if it is marked for checkpointing
    RDDCheckpointData.synchronized {
      if (cpState == MarkedForCheckpoint) {
        cpState = CheckpointingInProgress
      } else {
        return
      }
    }
    // Create the output path for the checkpoint
    val path = new Path(rdd.context.checkpointDir.get, "rdd-" + rdd.id)
    val fs = path.getFileSystem(rdd.context.hadoopConfiguration)
    if (!fs.mkdirs(path)) {
      throw new SparkException("Failed to create checkpoint path " + path)
    }

    // Save to file, and reload it as an RDD
    val broadcastedConf = rdd.context.broadcast(
      new SerializableWritable(rdd.context.hadoopConfiguration))
    /* runJob(rdd, iterator => something, result _, partition list, false) underscore=partially applied function*/
    rdd.context.runJob(rdd, CheckpointRDD.writeToFile[T](path.toString, broadcastedConf) _, 0 until rdd.partitions.size, false)
    
    val newRDD = new CheckpointRDD[T](rdd.context, path.toString)
    if (newRDD.partitions.size != rdd.partitions.size) {
      throw new SparkException(
        "Checkpoint RDD " + newRDD + "(" + newRDD.partitions.size + ") has different " +
          "number of partitions than original RDD " + rdd + "(" + rdd.partitions.size + ")")
    }

    // Change the dependencies and partitions of the RDD
    RDDCheckpointData.synchronized {
      cpFile = Some(path.toString)
      cpRDD = Some(newRDD)
      rdd.markCheckpointed(newRDD)   // Update the RDD's dependencies and partitions
      cpState = Checkpointed
    }
    logInfo("Done checkpointing RDD " + rdd.id + " to " + path + ", new parent is RDD " + newRDD.id)
  }

  // Get preferred location of a split after checkpointing
  def getPreferredLocations(split: Partition): Seq[String] = {
    RDDCheckpointData.synchronized {
      cpRDD.get.preferredLocations(split)
    }
  }

  def getPartitions: Array[Partition] = {
    RDDCheckpointData.synchronized {
      cpRDD.get.partitions
    }
  }

  def checkpointRDD: Option[RDD[T]] = {
    RDDCheckpointData.synchronized {
      cpRDD
    }
  }
}

// Used for synchronization
private[spark] object RDDCheckpointData
