DB_NAME="sales"

TABLE_NAME="sales_data"

OUTPUT_FILE="sales_data.sql"

#exportar dados da tabela pro arquivo sql
mysqldump -u root -p $DB_NAME $TABLE_NAME > $OUTPUT_FILE
