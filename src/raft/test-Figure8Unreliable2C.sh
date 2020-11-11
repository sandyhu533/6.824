# shellcheck disable=SC1072
for (( i = 0; i < 10; i++ )); do
  name="out-TestFigure8Unreliable2C-${i}";
  go test -run TestFigure8Unreliable2C > ${name}
  if [ "$(grep FAIL ${name})" == "" ]; then
      rm -f ${name}
      echo "test TestFigure8Unreliable2C  ${i} works fine"
  fi
done

