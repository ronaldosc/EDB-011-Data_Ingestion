import os
import mysql.connector

# Conexão com o banco de dados MySQL
db_config = {
    'host': os.getenv('MYSQL_HOST'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DB')
}

def main():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Execute uma consulta de exemplo
        cursor.execute("SELECT * FROM tabela_fato LIMIT 10")
        result = cursor.fetchall()

        for row in result:
            print(row)

        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")

if __name__ == "__main__":
    main()
