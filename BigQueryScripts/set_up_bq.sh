./Utils/create_encription_key.sh
./Utils/create_data_set.sh

for FILE in schemas/*; do
  name="${${FILE:8}:0:-12}";
  ./Utils/create_table_bq.sh $name $name "$FILE" "ts" DAY || ./Utils/update_table_bq.sh $name "$FILE"
done