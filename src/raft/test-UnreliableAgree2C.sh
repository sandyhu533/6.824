# shellcheck disable=SC1072
for (( i = 0; i < 10; i++ )); do
  name="out-TestUnreliableAgree2C-${i}";
  go test -run TestUnreliableAgree2C > ${name}
  if [ "$(grep FAIL ${name})" == "" ]; then
      rm -f ${name}
      echo "test TestUnreliableAgree2C  ${i} works fine"
  fi
done

