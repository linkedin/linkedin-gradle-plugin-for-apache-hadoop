--
-- Copyright 2015 LinkedIn Corp.
--
-- Licensed under the Apache License, Version 2.0 (the "License"); you may not
-- use this file except in compliance with the License. You may obtain a copy of
-- the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
-- WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
-- License for the specific language governing permissions and limitations under
-- the License.
--

-- Add the following line to the top of the script: if running at LinkedIn
-- use u_<username>;

-- Disable compression so we can easily examine the output on HDFS
set hive.exec.compress.output=false;

DROP TABLE lines;
DROP TABLE word_count;

CREATE TABLE lines (line STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH 'text' OVERWRITE INTO TABLE lines;

-- Store the counts as TEXTFILE in the given output location so we can easily examine the output.
-- We will split the lines on only the space character so that the output matches the output of the
-- Apache Pig and Java map-reduce jobs.
CREATE TABLE word_count
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '${outputPath}' AS
SELECT word, COUNT(*) as count FROM lines LATERAL VIEW explode(split(line, ' ')) lTable as word
GROUP BY word ORDER BY word;
