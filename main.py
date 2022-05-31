import random
import time
import psycopg2.extras
from faker import Faker

PGHOST = "localhost"
PGDATABASE = "postgres"
PGUSER = "postgres"
PGPASSWORD = "admin"

conn_string = "host=" + PGHOST + " port=" + "5432" + " dbname=" + PGDATABASE + " user=" + PGUSER + " password=" + PGPASSWORD
conn = psycopg2.connect(conn_string)

global end


def insert_into_teams(nr_of_rows):
    faker = Faker()
    team_names = []
    start = time.time()
    cursor = conn.cursor()

    teams_final_name_array = ["City", "United", "FC", "County", "Inter", ""]

    for i in range(nr_of_rows):
        if nr_of_rows > 80000:
            the_city = str(faker.city()).replace("'", "") + str(random.randint(0, 9999999))
        else:
            the_city = str(faker.unique.city()).replace("'", "")

        name_index = random.randint(0, len(teams_final_name_array) - 1)
        team_name = the_city + " " + teams_final_name_array[name_index]
        team_names.append(team_name)
        generated_city = "'" + the_city + "'"
        generated_name = "'" + team_name + "'"
        generated_country = "'" + str(faker.country().replace("'", "")) + "'"
        generated_points = random.randint(0, 132)

        insert_command = "INSERT INTO {}(name, country, city, points) VALUES ({}, {}, {}, {});".format(
            "licenta.teams", generated_name, generated_country, generated_city, generated_points)

        cursor.execute(insert_command, conn)
        end = time.time()

    conn.commit()
    cursor.close()
    print("Timp inserare echipe:", (end - start) * 1000)
    return team_names


def insert_into_players(nr_of_rows, team_names):
    faker = Faker()

    start = time.time()

    cursor = conn.cursor()

    positions_array = ["GK", "LB", "RB", "CB", "SW", "RWB", "LWB", "DM", "CM", "AM", "LM", "RM", "LW", "RW", "ST", "CF"]

    for i in range(nr_of_rows):
        team_index = random.randint(0, len(team_names) - 1)
        positions_index = random.randint(0, len(positions_array) - 1)

        generated_first_name = "'" + str(faker.first_name().replace("'", "")) + "'"
        generated_last_name = "'" + str(faker.last_name().replace("'", "")) + "'"
        generated_shirt_number = random.randint(0, 99)
        generated_position = "'" + positions_array[positions_index] + "'"
        generated_age = random.randint(14, 45)
        generated_team = "'" + team_names[team_index] + "'"

        insert_command = "INSERT INTO {}(first_name, last_name, shirt_number, position, age, " \
                         "team) VALUES ({}, {}, {}, {}, {}, {});".format(
            "licenta.players", generated_first_name, generated_last_name, generated_shirt_number, generated_position,
            generated_age, generated_team)

        cursor.execute(insert_command, conn)
        end = time.time()

    conn.commit()
    cursor.close()
    print("Timp inserare jucatori:", (end - start) * 1000)


if __name__ == '__main__':
    team_names = insert_into_teams(160000)
    insert_into_players(160000, team_names)
