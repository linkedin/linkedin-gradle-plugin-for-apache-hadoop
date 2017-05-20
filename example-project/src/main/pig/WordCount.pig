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

-- Word count job implemented in Apache Pig
lines = LOAD '$inputPath';

-- Tokenize using only a space, so that the results will match the Hive and Java map-reduce jobs
words = FOREACH lines GENERATE FLATTEN(TOKENIZE((chararray)$0)) AS word;
words = FILTER words BY word MATCHES '\\w+';

wordGroups = GROUP words BY word;
wordCounts = FOREACH wordGroups GENERATE group, COUNT(words);
wordOrders = ORDER wordCounts BY group;

RMF -skipTrash $outputPath;
STORE wordOrders INTO '$outputPath';
