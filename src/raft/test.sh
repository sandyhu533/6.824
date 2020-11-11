# shellcheck disable=SC1072
for (( i = 0; i < 3; i++ )); do {
    name="out-${i}";
  ct=0;
  go test > ${name}
  if [ "$(grep FAIL ${name})" == "" ];
  then
      rm -f ${name}
      ct=ct+1;
  else
    echo "test case union - ${i} failed!"
  fi;
  echo "[union] passed ${ct}/100 cases"
} &
done

unit_test_arr=("TestInitialElection2A" "TestReElection2A" "TestBasicAgree2B" "TestRPCBytes2B"
"TestFailAgree2B" "TestFailNoAgree2B" "TestConcurrentStarts2B" "TestRejoin2B" "TestBackup2B"
"TestCount2B" "TestPersist12C" "TestPersist22C" "TestPersist32C" "TestFigure82C" "TestUnreliableAgree2C"
"TestFigure8Unreliable2C")
for ((j = 0; j < ${#unit_test_arr[*]}; j++)); do {
  for (( i = 0; i < 3; i++ )); do
    name="out-${unit_test_arr[j]}-${i}";
    ct=0;
    go test > ${name}
    if [ "$(grep FAIL ${name})" == "" ];
    then
        rm -f ${name}
        ct=ct+1;
    else
      echo "test case ${unit_test_arr[j]} - ${i} failed!"
    fi;
    echo "[${unit_test_arr[j]}] passed ${ct}/100 cases"
  done
}&
done
wait