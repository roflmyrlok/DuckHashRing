for i in {1..100}; do
  curl -X POST 'http://127.0.0.1:52640/api/tables/string/read' \
    -H 'accept: */*' \
    -H 'Content-Type: application/json' \
    -d "{\"string\": $((72000 + i))}" \
    -w "\n" \
    -s &

done