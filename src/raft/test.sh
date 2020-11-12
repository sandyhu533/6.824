#!/bin/sh
ct=0;
for (( i = 0; i < 15; i++ )); do {
  name="out-${i}";
  echo "start running [union] case ${i}"
  go test > ${name}
  if [ "$(grep FAIL ${name})" == "" ];
  then
      # rm -f ${name}
      ct=$(($ct+1))
  else
    echo "test case union - ${i} failed!"
  fi;
  echo "[union] passed ${ct}/15 cases"
}
done

# unit_test_arr=("TestInitialElection2A" "TestReElection2A" "TestBasicAgree2B" "TestRPCBytes2B"
# "TestFailAgree2B" "TestFailNoAgree2B" "TestConcurrentStarts2B" "TestRejoin2B" "TestBackup2B"
# "TestCount2B" "TestPersist12C" "TestPersist22C" "TestPersist32C" "TestFigure82C" "TestUnreliableAgree2C"
# "TestFigure8Unreliable2C")
# for ((j = 0; j < ${#unit_test_arr[*]}; j++)); do {
#   ct=0;
#   for (( i = 0; i < 10; i++ )); do
#     name="out-${unit_test_arr[j]}-${i}";
#     echo "start running [${unit_test_arr[j]}] case ${i}"
#     go test > ${name}
#     if [ "$(grep FAIL ${name})" == "" ];
#     then
#         rm -f ${name}
#         ct=$(($ct+1))
#     else
#       echo "test case ${unit_test_arr[j]} - ${i} failed!"
#     fi;
#   echo "[${unit_test_arr[j]}] passed ${ct}/100 cases"
#   done
# } &
# done

wait
