# shellcheck disable=SC1072
for (( i = 0; i < 10; i++ )); do
  name="out-2A-${i}";
  go test -run 2A > ${name}
  if [ "$(grep FAIL ${name})" == "" ]; then
      rm -f ${name}
      echo "test 2A case ${i} works fine"
  fi
done

