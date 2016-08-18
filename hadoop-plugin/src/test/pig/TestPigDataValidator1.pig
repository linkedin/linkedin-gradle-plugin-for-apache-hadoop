--D = load './student_data.txt' USING PigStorage(',') as (id : int, firstname:chararray, lastname:chararray, phone:chararray, city:chararray);
D = load './student_data4.txt' USING PigStorage(',') as (id : int, firstname:chararray, lastname:chararray, phone:chararray, city:chararray);
B = load './student_data2.txt' USING PigStorage(',') as (id:int, firstname:chararray, lastname :chararray, phone:chararray, city:chararray);
C = UNiON D , B;