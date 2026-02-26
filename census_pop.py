import requests
import pandas as pd

# Census API key
API_KEY = "a831082cd0e15b927b943a1cdc4494afb57d816a"


def download_county_population():
    """
    Download county population for Missouri using ACS 1-year dataset.
    Note: ACS 1-year data was not released for 2020 due to COVID-19.
    """

    years = [2019, 2020]
    pop_all = []

    for year in years:

        url = f"https://api.census.gov/data/{year}/acs/acs1"

        params = {
            "get": "NAME,B01003_001E",
            "for": "county:*",
            "in": "state:29",  # Missouri FIPS
            "key": API_KEY
        }

        r = requests.get(url, params=params)

        if r.status_code != 200 or not r.text.startswith("["):
            print(f"Skipping year {year} (ACS 1-year not available)")
            continue

        data = r.json()
        df = pd.DataFrame(data[1:], columns=data[0])

        df["population"] = df["B01003_001E"].astype(int)
        df["county"] = df["NAME"].str.replace(" County, Missouri", "", regex=False)
        df["year"] = year

        df = df[["county", "population", "year"]]

        pop_all.append(df)

    pop_df = pd.concat(pop_all, ignore_index=True)

    return pop_df


def download_zip_population():
    """
    Download ZIP/ZCTA population using ACS 5-year dataset.
    """

    years = range(2017, 2025)
    pop_all = []

    for year in years:

        url = f"https://api.census.gov/data/{year}/acs/acs5"

        params = {
            "get": "NAME,B01003_001E",
            "for": "zip code tabulation area:*",
            "key": API_KEY
        }

        r = requests.get(url, params=params)

        if r.status_code != 200 or not r.text.startswith("["):
            print(f"Skipping year {year}")
            continue

        data = r.json()
        df = pd.DataFrame(data[1:], columns=data[0])

        df["population"] = pd.to_numeric(df["B01003_001E"], errors="coerce")
        df["zipcode"] = df["zip code tabulation area"]
        df["year"] = year

        df = df[["zipcode", "population", "year"]]

        pop_all.append(df)

    pop_df = pd.concat(pop_all, ignore_index=True)

    return pop_df


def main():

    print("Downloading county population...")
    county_pop = download_county_population()
    print(county_pop.head())

    print("\nDownloading ZIP population...")
    zip_pop = download_zip_population()
    print(zip_pop.head())

    # Save results
    county_pop.to_csv("county_population_missouri.csv", index=False)
    zip_pop.to_csv("zip_population_acs5.csv", index=False)

    print("\nFiles saved successfully.")


if __name__ == "__main__":
    main()
