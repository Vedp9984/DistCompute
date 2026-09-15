# Hadoop 3.3.6 + YARN on the RCE Slurm cluster

RCE has no running YARN (a system Hadoop 3.3.0 install exists with a shared
HDFS, but its ResourceManager is down and the version is not the required
3.3.6). These scripts bring up a **private Hadoop 3.3.6 cluster inside a Slurm
allocation**, run a workload on it, and tear it down.

## One-time setup

```bash
mkdir -p ~/apps && cd ~/apps
curl -O https://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar xzf hadoop-3.3.6.tar.gz && rm hadoop-3.3.6.tar.gz
```

(`archive.apache.org` serves at ~20 kB/s from the campus network; the mirror
is ~1 MB/s.) Java comes from `module load java/11.0.13`
(`/usr/local/apps/jdk-11.0.13+8`); `env.sh` sets `JAVA_HOME` to it.

## Files

| File | Purpose |
|---|---|
| `env.sh` | `source` it: `HADOOP_HOME`, `JAVA_HOME`, `HADOOP_CONF_DIR` (→ `conf-live`), `STREAMING_JAR`, `PATH` |
| `gen_config.sh CONF MASTER "n1 n2 …" [vcores] [mem_mb]` | writes `core/hdfs/yarn/mapred-site.xml`, `workers`, env files |
| `node_daemons.sh MASTER` | runs on every node: NameNode + ResourceManager + JobHistory on the master, DataNode + NodeManager everywhere |
| `cluster.sbatch` | the job: generate config → start daemons → wait for readiness → run `$WORKLOAD` → hold → stop |
| `run_on_master.sh JOBID 'cmd'` | run a command on a held cluster's master from the login node |
| `smoke.sh` | workload that runs the SSSP sample and the weather hand-made test end to end |
| `bench_all.sh` | workload that runs both benchmark suites |

## Usage

```bash
cd ~/HW3
sbatch --export=ALL,WORKLOAD=hadoop/smoke.sh --nodes=2 --cpus-per-task=8  --mem=32G  --time=00:30:00 hadoop/cluster.sbatch
sbatch --export=ALL,WORKLOAD=hadoop/bench_all.sh --nodes=4 --cpus-per-task=16 --mem=100G --time=08:00:00 hadoop/cluster.sbatch
sbatch --export=ALL,HOLD_SECONDS=7200 hadoop/cluster.sbatch      # keep a cluster up for interactive use
bash hadoop/run_on_master.sh <jobid> 'hdfs dfs -ls /user/$USER'   # …and talk to it
```

Logs: `logs/hadoop_<jobid>.log`. Daemon logs: `/tmp/$USER/hadoop-<jobid>/logs`
on each node (deleted at teardown).

## Design decisions

* **Daemons run in the foreground inside one long `srun` step.** Slurm reaps
  every process of a step when the step ends, so `hdfs --daemon start`
  (which forks and returns) would be killed immediately. `node_daemons.sh`
  starts each daemon as a foreground child, `wait`s on them, and that step
  lives for the whole job. The batch script itself (on the master node) is
  the driver.
* **All state on node-local disk** (`/tmp/$USER/hadoop-<jobid>`): NameNode
  metadata, DataNode blocks, YARN local/log dirs, MapReduce spill. `/scratch`
  is not mounted on every node and the NFS home would make every block write
  a network write. Replication is 1: the cluster lives for one job.
* **Ports moved off the defaults** (block 21000–21099): compute nodes are
  shared, and two students' clusters on the same node must not collide on
  9000 / 8088 / 13562.
* **Resources.** `--cpus-per-task` becomes `yarn.nodemanager.resource.cpu-vcores`;
  YARN memory is `--mem` minus 8 GB for the daemons; containers are 2 GB
  (map) / 3 GB (reduce); the disk-health threshold is raised to 99 % because
  the login node's root disk sits at 94 %.
* **Readiness is polled**, not assumed: the driver waits until
  `hdfs dfsadmin -report` shows every DataNode and `yarn node -list` every
  NodeManager, then leaves safe mode, before running the workload.
* **Per-job overhead.** An empty Streaming job costs ~18 s here (container
  allocation, two JVM start-ups, the ApplicationMaster's own life-cycle).
  That number appears everywhere in the Section 1 and Section 2 analyses.
