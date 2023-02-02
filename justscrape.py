import sys
import re
import time

import csv
import json
import pathlib
import urllib.request

import concurrent.futures


def progress_bar(
    iteration, total, prefix="", suffix="", decimals=1, length=20, fill="🍕"
) -> None:
    """
    Call in a loop to create a progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + "  " * (length - filled_length)
    sys.stdout.write("\r%s |%s| %s%% %s" % (prefix, bar, percent, suffix))
    sys.stdout.flush()
    if iteration == total:
        print()


def get_zip_codes(city) -> tuple:
    """
    Retrieves the zip codes for a given city from paginebianche.it
    @params:
        city        - Required  : city name (Str)
    @returns:
        city_name   - Required  : city name (Str)
        zip_bounds  - Required  : zip code bounds (tuple)
    """

    pb_url = f"https://www.paginebianche.it/cap?dv={city}"

    result = urllib.request.urlopen(pb_url).read()

    city_name = re.findall(rb'<b class="capitalize">(.*?)</b>', result, re.DOTALL)[
        0
    ].decode("utf-8")
    print(f"Collecting zip codes for {city_name}")

    city_name = city_name.replace(" ", "")

    result = re.findall(
        rb'<span class="(.*?)result-cap(.*?)">(.*?)</span>', result, re.DOTALL
    )[0]
    result = result[2].decode("utf-8")
    result = re.findall(rb"<a(.*?)>(.*?)</a>", result.encode("utf-8"), re.DOTALL)

    zip_bounds = (
        result[0][1].decode("utf-8"),
        result[result.count("<a") - 1][1].decode("utf-8"),
    )

    if zip_bounds[0] == zip_bounds[1]:
        print(f"Found zip code {zip_bounds[0]}")
    else:
        print(f"Found zip codes from {zip_bounds[0]} to {zip_bounds[1]}")

    return (city_name, zip_bounds)


def intialize_directory(city, time_stamp) -> pathlib.Path:
    """
    Creates a directory for the current scrape
    @params:
        city        - Required  : city name (Str)
        time_stamp  - Required  : time stamp (Str)
    @returns:
        project_dir - Required  : project directory (pathlib.Path)
    """
    main_dir = pathlib.Path(__file__).parent

    project_dir = main_dir / f"{city}_{time_stamp}"
    project_dir.mkdir(parents=True, exist_ok=True)

    return project_dir


def get_justeat_data(zip_code) -> str:
    """
    Retrieves the data for a given zip code from justeat.it
    @params:
        zip_code    - Required  : zip code (Str)
    @returns:
        data        - Required  : data (Str)
    """
    je_url = f"https://www.justeat.it/area/{zip_code}"
    request = urllib.request.Request(je_url)
    request.add_header("User-Agent", "Mozilla/5.0")
    response = urllib.request.urlopen(request).read()

    scripts = re.findall(rb"<script>(.*?)</script>", response, re.DOTALL)
    for script in scripts:
        if script.startswith(b'window["__INITIAL_STATE__"]'):
            data = script.decode("utf-8")
            break

    return data


def parse_justeat_data(data) -> dict:
    """
    Parses the data retrieved from justeat.it
    @params:
        data        - Required  : data (Str)
    @returns:
        data        - Required  : data (dict)
    """

    data = data[(data.index("(") + 1) : (data.rindex(")"))]

    data = data.replace("false", "False")
    data = data.replace("true", "True")
    data = data.replace("null", "None")
    data = data.replace("undefined", "None")
    data = data.replace("NaN", "None")
    data = data.replace("Infinity", "None")
    data = data.replace("-Infinity", "None")

    data = json.loads(data)

    return eval(data)


def parse_restaurant_data(idx, data) -> dict:
    """
    Parses the restaurant data retrieved from justeat.it
    @params:
        idx         - Required  : restaurant index (Int)
        data        - Required  : data (dict)
    @returns:
        restaurant_data - Required  : restaurant data (dict)
    """

    restaurant = data["restaurants"].get(idx, {})
    times = data["restaurantTimes"].get(idx, {})
    ratings = data["ratings"].get(idx, {})
    cuisines = data["restaurantCuisines"].get(idx, [])
    analytics = data["additionalAnalytics"]["restaurantAnalytics"].get(idx, {})
    restaurant_data = {
        "id": idx,
        "name": restaurant.get("name", ""),
        "uniqueName": restaurant.get("uniqueName", ""),
        "isTemporaryBoost": restaurant.get("isTemporaryBoost", ""),
        "isTemporarilyOffline": restaurant.get("isTemporarilyOffline", ""),
        "isPremier": restaurant.get("isPremier", ""),
        "defaultPromoted": data.get("promotedPlacement", {})
        .get("defaultPromotedRestaurants", {})
        .get(idx, {})
        .get("defaultPromoted", ""),
        "isNew": restaurant.get("isNew", ""),
        "position": restaurant.get("position", ""),
        "deliveryCost": analytics.get("deliveryCost", ""),
        "minimumDeliveryValue": analytics.get("minimumDeliveryValue", ""),
        "address": restaurant.get("address", ""),
        "starRating": ratings.get("starRating", ""),
        "ratingCount": ratings.get("ratingCount", ""),
        "isOpenNowForCollection": times.get("isOpenNowForCollection", ""),
        "isOpenNowForDelivery": times.get("isOpenNowForDelivery", ""),
        "isOpenNowForPreOrder": times.get("isOpenNowForPreOrder", ""),
        "nextOpeningTime": times.get("nextOpeningTime", ""),
        "nextDeliveryTime": times.get("nextDeliveryTime", ""),
        "cuisineTypes_1": cuisines[0] if cuisines else "",
        "cuisineTypes_2": cuisines[1] if len(cuisines) >= 2 else "",
    }

    return restaurant_data


def zip_data_to_csv(zip_data, project_dir, city, zip_code, time_stamp) -> None:
    """
    Saves the data for a given zip code to a csv file
    @params:
        zip_data    - Required  : zip data (dict)
        project_dir - Required  : project directory (pathlib.Path)
        city        - Required  : city name (Str)
        zip_code    - Required  : zip code (Str)
        time_stamp  - Required  : time stamp (Str)
    """

    keys = zip_data[0].keys()
    with open(
        project_dir / f"{city}_{zip_code}_{time_stamp}.csv", "w", newline=""
    ) as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(zip_data)


def data_to_csv(city_data, project_dir, city, time_stamp) -> None:
    """
    Saves the data for a given city to a csv file
    @params:
        city_data   - Required  : city data (dict)
        project_dir - Required  : project directory (pathlib.Path)
        city        - Required  : city name (Str)
        time_stamp  - Required  : time stamp (Str)
    """

    keys = city_data[0].keys()
    with open(project_dir / f"{city}_{time_stamp}.csv", "w", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(city_data)


def remove_duplicates(city_data) -> list:
    """
    Removes duplicates from the data for a given city
    @params:
        city_data   - Required  : city data (dict)
    @returns:
        unique_data - Required  : unique data (list)
    """

    from collections import defaultdict

    groups = defaultdict(list)
    for r in city_data:
        groups[r["id"]].append(r)

    unique_data = []
    for idx, group in groups.items():
        avg_position = sum(r["position"] for r in group) / len(group)
        avg_delivery_cost = sum(r["deliveryCost"] for r in group) / len(group)

        for r in group:
            r["averagePosition"] = avg_position
            r["averageDeliveryCost"] = avg_delivery_cost
            unique_data.append(
                {
                    k: v
                    for k, v in r.items()
                    if k
                    not in (
                        "isTemporaryBoost",
                        "isTemporarilyOffline",
                        "isPremier",
                        "defaultPromoted",
                        "isNew",
                        "isOpenNowForCollection",
                        "isOpenNowForDelivery",
                        "isOpenNowForPreOrder",
                        "nextOpeningTime",
                        "nextDeliveryTime",
                        "position",
                        "deliveryCost",
                    )
                }
            )

    return unique_data


def process_zip_code(zip_data, project_dir, city, zip_code, time_stamp) -> tuple:
    """
    Processes the data for a given zip code
    @params:
        zip_data    - Required  : zip data (dict)
        project_dir - Required  : project directory (pathlib.Path)
        city        - Required  : city name (Str)
        zip_code    - Required  : zip code (Str)
        time_stamp  - Required  : time stamp (Str)
    @returns:
        zip_code    - Required  : zip code (Str)
        zip_data    - Required  : zip data (dict)
    """
    zip_data = []
    data = get_justeat_data(zip_code)
    data = parse_justeat_data(data)

    for idx in list(data["restaurants"].keys()):
        restaurant_data = parse_restaurant_data(idx, data)
        zip_data.append(restaurant_data)

    if len(zip_data) > 0:
        zip_data_to_csv(zip_data, project_dir, city, zip_code, time_stamp)

    return zip_code, zip_data


def parallel_processing(zip_codes) -> list:
    """
    Processes the data for a given city in parallel
    @params:
        zip_codes   - Required  : zip codes (list)
    @returns:
        city_data   - Required  : city data (dict)
    """

    city_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(
                process_zip_code, zip_code, project_dir, city, zip_code, time_stamp
            ): zip_code
            for zip_code in zip_codes
        }
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            zip_code, zip_data = future.result()
            city_data.extend(zip_data)
            progress_bar(
                i + 1,
                len(zip_codes),
                prefix=f"Processing zip code {zip_code}",
                suffix="Complete",
            )

    return city_data


if __name__ == "__main__":

    city = input("Insert city name:")
    city = city.replace(" ", "+")

    city, zip_bounds = get_zip_codes(city)
    time_stamp = time.strftime("%Y%m%d-%H%M%S")
    project_dir = intialize_directory(city, time_stamp)

    city_data = []

    zip_codes = range(int(zip_bounds[0]), int(zip_bounds[1]) + 1)
    city_data = parallel_processing(zip_codes)
    print("Removing duplicates...")
    city_data = remove_duplicates(city_data)
    print(f"Saving city data to {city}_{time_stamp}.csv")
    data_to_csv(city_data, project_dir, city, time_stamp)
