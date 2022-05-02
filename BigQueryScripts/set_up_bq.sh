
bq ls | awk '/results/{print}' | wc -c | read exists
if [[ $exists == 0 ]]; then
./Utils/create_encription_key.sh
./Utils/create_data_set.sh
fi

for FILE in schemas/*; do
  name="${${FILE:8}:0:-12}";
  bq ls results | awk '/ *_aggregation/{print}' | wc -c | read exists
  if [[ $exists != 0 ]]; then
    ./Utils/update_table_bq.sh $name "$FILE"
  else
    ./Utils/create_table_bq.sh $name $name "$FILE" "ts" DAY
  fi
done