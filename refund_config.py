import logging
class Config:
    CSV_PATH = "/Users/thatguy/de-venv/data/refund_orders.csv"
    MYSQL_URL = "jdbc:mysql://localhost:3306/refund_db"
    MYSQL_PROPS = {"user": "root", "password": "", "driver": "com.mysql.cj.jdbc.Driver"}
    SALT_COUNT = 5
    SKEW_THRESHOLD = 500

logging.basicConfig(
    level= logging.INFO,
    format= "%(asctime)s [%(levelname)s] %(message)s",
    handlers= [
        logging.FileHandler("refund_etl.log"),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

class PipelineError(Exception):
    """管道异常"""