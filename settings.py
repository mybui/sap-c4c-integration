from environs import Env

env = Env()
env.read_env()

ELQ_USER = env("ELQ_USER")
ELQ_PASSWORD = env("ELQ_PASSWORD")
ELQ_BASE_URL = env("ELQ_BASE_URL")

C4C_USER = env("C4C_USER")
C4C_PASSWORD = env("C4C_PASSWORD")
C4C_BASE_URL = env("C4C_BASE_URL")

DB_NAME = env("DB_NAME")
DB_USER = env("DB_USER")
DB_PASSWORD = env("DB_PASSWORD")
DB_HOST_NAME = env("DB_HOST_NAME")
DB_URI = "postgresql+psycopg2://" + DB_USER + ":" + DB_PASSWORD + "@" + DB_HOST_NAME + "/" + DB_NAME
