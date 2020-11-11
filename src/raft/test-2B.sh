# shellcheck disable=SC1072
for (( i = 0; i < 10; i++ )); do
  name="out-2B-${i}";
  go test -run 2B > ${name}
  if [ "$(grep FAIL ${name})" == "" ]; then
      rm -f ${name}
      echo "test 2B case ${i} works fine"
  fi
done

