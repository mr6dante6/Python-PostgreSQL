import psycopg2


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE if not exists clients (
                client_id SERIAL PRIMARY KEY,
                first_name VARCHAR(20) NOT NULL,
                last_name VARCHAR(20) NOT NULL,
                email VARCHAR(60) NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE if not exists phones (
                phone_id SERIAL PRIMARY KEY,
                phone_number numeric(11) UNIQUE,
                client_id integer not null references clients(client_id)
            )
        """)
        conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clients (first_name, last_name, email)
            VALUES (%s, %s, %s)
            RETURNING client_id;
        """, (first_name, last_name, email))
        client_id = cur.fetchone()[0]
        if phones:
            if type(phones) == list:
                for ph in phones:
                    cur.execute("""
                        INSERT INTO phone (phone_number, client_id)
                        VALUES (%s, %s)
                    """, (ph, client_id))
            else:
                cur.execute("""
                    INSERT INTO phones (phone_number, client_id)
                    VALUES (%s, %s)
                """, (phones, client_id))

        conn.commit()


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        if type(phone) == list:
            for ph in phone:
                cur.execute("""
                    INSERT INTO phone (phone_number, client_id)
                    VALUES (%s, %s)
                """, (ph, client_id))
        else:
            cur.execute("""
                INSERT INTO phones (phone_number, client_id)
                VALUES (%s, %s)
            """, (phone, client_id))


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        if first_name:
            cur.execute("""
                UPDATE clients SET first_name = %s
                WHERE client_id = %s
            """, (first_name, client_id))
        if last_name:
            cur.execute("""
                UPDATE clients SET last_name = %s
                WHERE client_id = %s
            """, (last_name, client_id))
        if email:
            cur.execute("""
                UPDATE clients SET email = %s
                WHERE client_id = %s
            """, (email, client_id))
        if phones:
            cur.execute("""
                        SELECT COUNT(*) FROM phones
                        WHERE client_id = %s
                    """, (client_id,))
            num_phones = cur.fetchone()[0]

            if num_phones == 0:
                add_phone(conn, client_id, phones)
            elif num_phones == 1:
                cur.execute("""
                            UPDATE phones SET phone_number = %s
                            WHERE client_id = %s
                        """, (phones, client_id))
            else:
                add_phone(conn, client_id, phones)
        conn.commit()


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM phones WHERE client_id = %s AND phone_number = %s", (client_id, phone))
        conn.commit()


def delete_client(conn, client_id):
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM clients
                WHERE client_id = %s;
                """,
                (client_id,)
            )
            print(f"Клиент с ID {client_id} был удалён из базы данных.")
    except Exception as e:
        print(f"{e}")


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        query = """
            SELECT c.*, array_agg(p.phone_number) AS phones
            FROM clients c
            LEFT JOIN phones p ON c.client_id = p.client_id
            WHERE """
        conditions = []
        params = []
        if first_name:
            conditions.append("c.first_name ILIKE %s")
            params.append(f"%{first_name}%")
        if last_name:
            conditions.append("c.last_name ILIKE %s")
            params.append(f"%{last_name}%")
        if email:
            conditions.append("c.email ILIKE %s")
            params.append(f"%{email}%")
        if phone:
            conditions.append("p.phone_number = %s")
            params.append(phone)
        if not conditions:
            raise ValueError("At least one search parameter is required")
        query += " AND ".join(conditions)
        query += " GROUP BY c.client_id"
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        for row in rows:
            client_id, first_name, last_name, email, phones = row
            phones = [int(phone) for phone in phones]
            print(client_id, first_name, last_name, email, phones)


with psycopg2.connect(database='my_db', user='postgres', password='DiMoN1991') as conn:
    """Функция, создающая структуру БД (таблицы)"""
    create_db(conn)

    """Функция, позволяющая добавить нового клиента"""
    # add_client(conn, "Иван", "Ивaнов", "ivan@example.com", phones="79993285943")
    # add_client(conn, "Сергей", "Сергеев", "sergey@example.com", phones="79995843868")
    # add_client(conn, "Алексей", "Алексеев", "alexeev@example.com", phones="79134532058")
    # add_client(conn, "Павел", "Павлов", "pavlov@example.com", phones="79052495834")

    """Функция, позволяющая добавить телефон для существующего клиента"""
    # add_phone(conn, 1, '79234536496')
    # add_phone(conn, 3, '79094342965')
    # add_phone(conn, 2, '79094442965')

    """Функция, позволяющая изменить данные о клиенте"""
    # change_client(conn, 2, email='sergey@example.ru')
    # change_client(conn, 3, last_name='Петров')
    # change_client(conn, 1, phones='79535943945')
    # change_client(conn, 3, phones='79535965953')
    # change_client(conn, 2, phones='79535958853')

    """Функция, позволяющая удалить телефон для существующего клиента"""
    # delete_phone(conn, 2, '79535958853')
    # delete_phone(conn, 2, '79094442965')

    """Функция, позволяющая удалить существующего клиента"""
    # delete_client(conn, 3)

    """Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону"""
    # find_client(conn, email='ivan@example.com')

conn.close()
