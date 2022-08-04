from typing import Any
import ijson

from db.mongo.database import IndagoSession

def main(args: Any):
    collection = IndagoSession[args.collection]

    with open(args.json_file, "rb") as f:
        counter = 0
        tmp_list = []
        for record in ijson.items(f, "item"):
            tmp_list.append(record)
            counter += 1
            if counter % args.batch_size == 0:
                print(f"{counter} records processed...")
                collection.insert_many(tmp_list)
                tmp_list = []
        print("Done!")

if __name__ == "__main__":
    from argparse import ArgumentParser
    parser: ArgumentParser = ArgumentParser()
    parser.add_argument(
        'json_file',
        type=str,
        help='path to json to upload',
    )
    parser.add_argument(
        '--collection',
        type=str,
        help='name of collection to upload to',
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        help="json objects to upload per batch",
        default=10000
    )
    args: Any = parser.parse_args()

    main(args)