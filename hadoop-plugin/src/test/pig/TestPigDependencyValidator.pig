--D = load './student_data.txt' USING PigStorage(',') as (id : int, firstname:chararray, lastname:chararray, phone:chararray, city:chararray);
D = load './student_data4.txt' USING PigStorage(',') as (id : int, firstname:chararray, lastname:chararray, phone:chararray, city:chararray);
RegisTer hdfs://abracadabra:9000/student_data.jar ;register ivy://com.linkedin.pig:pig:0.15.0.14;
B = load './student_data2.txt' USING PigStorage(',') as (id:int, firstname:chararray, lastname :chararray, phone:chararray, city:chararray);
RegisTer file:/abracadabra/student_data.jar;
C = UNiON D , B;/*vsfvsfv*sfvsf*vfdvb*/
regISTER fooBar.py;