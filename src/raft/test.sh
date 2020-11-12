#!/bin/sh

unit_test_arr=("2A" "TestBasicAgree2B" "TestRPCBytes2B"
 "TestFailAgree2B" "TestFailNoAgree2B" "TestConcurrentStarts2B" "TestRejoin2B" "TestBackup2B"
 "TestCount2B" "TestPersist12C" "TestPersist22C" "TestPersist32C" "TestFigure82C" "TestUnreliableAgree2C"
 "TestFigure8Unreliable2C" "TestReliableChurn2C" "TestUnreliableChurn2C")

for (( r = 0; r < 100; r++ )); do {
  for ((j = 0; j < ${#unit_test_arr[*]}; j++)); do {
    ct=0;
    for (( i = 0; i < 5; i++ )); do {
      name="out-${unit_test_arr[j]}-$r-$i";
      echo "start running [${unit_test_arr[j]}] round $r case ${i}"
      go test -run ${unit_test_arr[j]} > ${name}
      if [ "$(grep FAIL ${name})" == "" ];
      then
        rm -f ${name}
        ct=$(($ct+1))
        echo "test case ${unit_test_arr[j]} - $r - ${i} passed!"
      else
        echo "test case ${unit_test_arr[j]} - $r - ${i} failed!"
      fi;
   } &
   done
   wait
  }
  done
  wait
}
done
