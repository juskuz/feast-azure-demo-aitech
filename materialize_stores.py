import datetime

from utils.prepare_feast import create_fs


def main():
    fs = create_fs()

    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(hours=1)
    fs.materialize(start_date=start_date, end_date=end_date)



if __name__ == "__main__":
    main()
