import json

dataDir = "imbd-reviews/"
files_to_load = ["part-02"] # , "part-02", "part-03", "part-04", "part-05", "part-06"

for file in files_to_load:
    print(f"Starting to process {file}.")
    with open(f"{dataDir}{file}.json", mode="r") as fr:
        reviews = json.load(fr)
        i = 0
        for j, review in enumerate(reviews):
            if j % 2000 == 0:
                i += 1
                print(f"{j} reviews processed in file {file}.")
            with open(f"{dataDir}{file}/{file}-{i}.json", "a") as fw:
                json.dump(review, fw)