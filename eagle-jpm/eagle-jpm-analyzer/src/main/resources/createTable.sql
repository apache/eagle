CREATE TABLE IF NOT EXISTS sla_jobs (
  jobDefId VARCHAR(50) NOT NULL,
  configuration MEDIUMTEXT NOT NULL,
  createdtime bigint(20) DEFAULT NULL,
  modifiedtime  bigint(20) DEFAULT NULL,
  PRIMARY KEY (jobDefId)
);

CREATE TABLE IF NOT EXISTS sla_job_evaluators (
  jobDefId VARCHAR(50) NOT NULL,
  evaluator VARCHAR(100) NOT NULL,
  createdtime bigint(20) DEFAULT NULL,
  modifiedtime  bigint(20) DEFAULT NULL,
  PRIMARY KEY (jobDefId, evaluator)
);

CREATE TABLE IF NOT EXISTS sla_publishments (
  userId VARCHAR(100) PRIMARY KEY,
  mailAddress mediumtext NOT NULL,
  createdtime bigint(20) DEFAULT NULL,
  modifiedtime  bigint(20) DEFAULT NULL,
  PRIMARY KEY (userId)
);