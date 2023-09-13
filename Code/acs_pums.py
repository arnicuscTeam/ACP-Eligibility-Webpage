import os

import pandas as pd


# Function to crosswalk PUMS data from 2012 pumas to 2020 pumas
def crosswalkPUMAData(df: pd.DataFrame, crosswalk_dict: dict, source_column: str, target_column: str) -> pd.DataFrame:
    """
    This function will crosswalk the pums data from puma to another geography. It does so by reading the crosswalk file
    and creating a dictionary with the puma codes as keys and the new geography codes as values. It then iterates
    through the dictionary and multiplies the data by the afact. It then aggregates the data by the new geography code.
    :param df: The dataframe with the puma data
    :param crosswalk_dict: The dictionary with the crosswalk data
    :param source_column: The column name for the source geography, which would be the puma column
    :param target_column: The column name for the target geography, which would be the new geography column
    :return: A dataframe with the puma data crosswalked to the new geography
    """

    columns = df.columns.tolist()

    # Replace the code column with the new code column
    columns[0] = target_column

    # Initialize a dictionary to store the original data
    puma_data = {}

    # Group the data by the code column
    data_groupby = df.groupby(source_column)

    # Iterate through every code in the crosswalk file
    for puma in df[source_column].unique():
        # Get the data for the puma Code
        data = data_groupby.get_group(puma)

        # Convert the data to a list
        data = data.values.tolist()

        # Get the data for the puma
        row = data[0][1:]

        # Remove the period from the puma
        try:
            puma = puma.split(".")[0]
        except:
            pass
        puma = puma.zfill(7)

        # Add the puma and data to the dictionary
        puma_data[puma] = row

    # Create a new list to store the new data
    new_list = []

    # Iterate through every code in the crosswalk file
    for key, value in crosswalk_dict.items():
        # Iterate through every value in the list
        for val in value:
            # Get the puma and afact
            puma = str(val[0])

            # Remove the period from the puma
            try:
                puma = puma.split(".")[0]
            except:
                pass
            puma = puma.zfill(7)

            # Get the afact
            afact = val[1]

            # If the puma is in the dictionary, then calculate the new number eligible and ineligible
            if puma in puma_data.keys():
                temp_list = [key]
                for d in puma_data[puma]:
                    temp_list.append(int(round(d * afact)))
                new_list.append(temp_list)

    # Create a new dataframe with the new list
    new_df = pd.DataFrame(new_list, columns=columns)

    # Group the data by the code column
    new_df = new_df.groupby(target_column).sum()

    # Reset the index
    new_df.reset_index(inplace=True)

    # Return the new dataframe
    return new_df


# Function to crosswalk PUMS data from 2012 pumas to 2020 pumas
def crossWalkOldPumaNewPuma(df: pd.DataFrame, crosswalk_file: str) -> pd.DataFrame:
    """
    This function will crosswalk the PUMS data from 2012 pumas to 2020 pumas. It does so by reading the crosswalk file
    and creating a dictionary with the 2012 puma codes as keys and the 2020 puma codes as values. It then iterates
    through the dictionary and multiplies the data by the afact. It then aggregates the data by the 2020 puma code.
    :param df: The dataframe with the eligibility data
    :param crosswalk_file: The path to the crosswalk file
    :return: A dataframe with the eligibility data crosswalked to 2020 pumas
    """

    # Clean the puma column
    df["puma22"] = df["puma22"].astype(str)
    df["puma22"] = df["puma22"].str.zfill(7)

    # Drop the percentage eligible column
    df = df.drop(columns=["Percentage Eligible"])

    # Store the columns in a list
    columns = df.columns.tolist()

    # Get the dictionary from the crosswalk file
    dictionary, col = code_to_source_dict(crosswalk_file, "puma12")
    # Dict: {puma22: [(puma12, afact), (puma12, afact), (puma12, afact)]}

    # Turn the df into a dictionary
    df_dict = df.set_index("puma22").T.to_dict("list")
    # dict: {puma12: [eligible]}

    new_df = pd.DataFrame()

    # Crosswalk the puma12 to puma22

    # Iterate through the dictionary keys
    for puma22 in dictionary.keys():

        # Iterate through the tuples in the dictionary
        for tup in dictionary[puma22]:
            # Initialize the new data list
            new_data = []
            # Get the puma12 and afact
            puma12 = tup[0]
            afact = tup[1]

            # Check if the puma12 is in the df_dict
            if puma12 in df_dict.keys():
                # Get the data
                data = df_dict[puma12]

                # Add the puma22 to the new data list
                new_data.append(puma22)

                # Iterate through the data and multiply it by the afact
                for d in data:
                    # Multiply the data by the afact and round it, then add it to the new data list
                    new_data.append(int(round(d * afact)))

                # Create a dataframe with the new data
                temp_df = pd.DataFrame([new_data], columns=columns)

                # Add the dataframe to the new dataframe
                new_df = pd.concat([new_df, temp_df], axis=0)

    # Aggregate the data by the puma
    new_df = new_df.groupby(["puma22"]).sum()

    # Reset the index
    new_df.reset_index(inplace=True)

    # Zero fill the code column
    new_df["puma22"] = new_df["puma22"].str.zfill(7)

    # Calculate the percentage eligible
    new_df["Percentage Eligible"] = new_df["Num Eligible"] / (new_df["Num Eligible"] + new_df["Num Ineligible"]) * 100

    # Round the percentage eligible to two decimal places
    new_df["Percentage Eligible"] = new_df["Percentage Eligible"].round(2)

    # Move the percentage eligible column to the 4th column
    cols = new_df.columns.tolist()
    cols = cols[:3] + cols[-1:] + cols[3:-1]
    new_df = new_df[cols]

    # Delete variables that are no longer needed
    del df_dict, dictionary, df

    return new_df


def code_to_source_dict(crosswalk_file: str, source_col: str):
    code_col = ""

    # Get the column name for the source geography
    df_zeros = pd.read_csv(crosswalk_file)
    for col in df_zeros.columns.tolist():
        if source_col in col:
            source_col = col
            break

    # Read the crosswalk file to get the column names
    df_zeros = pd.read_csv(crosswalk_file, dtype={source_col: str})

    # Read the column names
    col_names = df_zeros.columns.tolist()
    col_names.remove(source_col)

    # Find the column with the target codes
    for col in col_names:
        if "zcta" in col:
            code_col = col
            break
        elif "county" in col and "tract" not in col_names:
            code_col = col
            break
        elif "metdiv" in col:
            code_col = col
            break
        elif "puma" in col:
            code_col = col
            break
        elif "tract" in col:
            code_col = col
            break
        elif "cd" in col:
            code_col = col
            break
        elif "sdbest" in col:
            code_col = col
            break
        elif "sdelem" in col:
            code_col = col
            break
        elif "sdsec" in col:
            code_col = col
            break
        elif "sduni" in col:
            code_col = col
            break
        elif "state" in col:
            code_col = col
            break

    try:
        df = pd.read_csv(crosswalk_file, dtype={source_col: str, code_col: str})
    except:
        df = pd.read_csv(crosswalk_file, dtype={source_col: str})

    # Group the source by code
    cw_lists = df.groupby(code_col)[source_col].apply(list)

    code_zcta_dict = {}

    for index, row in cw_lists.items():
        county_afact = []
        for item in row:
            # Find the afact on the crosswalk dataframe by the source code and the target code
            afact = df.loc[(df[source_col] == item) & (df[code_col] == index), 'afact'].iloc[0]

            tup = (str(item), afact)

            county_afact.append(tup)

        code_zcta_dict[str(index)] = county_afact

    return code_zcta_dict, code_col


# Function to conduct testing on the eligibility criteria
def determine_eligibility(data_dir: str, povpip: int = 200, has_pap: int = 1, has_ssip: int = 1, has_hins4: int = 1,
                          has_snap: int = 1, geography: str = "Public-use microdata area (PUMA)",
                          aian: int = 0, asian: int = 0, black: int = 0, nhpi: int = 0, white: int = 0,
                          hispanic: int = 0,
                          veteran: int = 0, elderly: int = 0, disability: int = 0, eng_very_well: int = 0):
    """
    This function will determine eligibility for ACP for all states. It does so by iterating through all the states and
    reading the eligibility data for each state. It will then aggregate the data by the geography specified. It will
    then save the data to a csv file in the state folder.
    :param data_dir: The path to the data directory which contains the ACS_PUMS folder
    :param povpip: The desired income threshold
    :param has_pap: Whether to use the PAP criteria 0|1
    :param has_ssip: Whether to use the SSIP criteria 0|1
    :param has_hins4: Whether to use the HINS4 criteria 0|1
    :param has_snap: Whether to use the SNAP criteria 0|1
    :param geography: The geography to aggregate the data by
    :param aian: Whether we want to see the effects to the American Indian and Alaska Native population 0|1
    :param asian: Whether we want to see the effects to the Asian population 0|1
    :param black: Whether we want to see the effects to the Black or African American population 0|1
    :param nhpi: Whether we want to see the effects to the Native Hawaiian population 0|1
    :param white: Whether we want to see the effects to the White population 0|1
    :param hispanic: Whether we want to see the effects to the Hispanic or Latino population 0|1
    :param veteran: Whether we want to see the effects to the Veteran population 0|1
    :param elderly: Whether we want to see the effects to the Elderly population 0|1
    :param disability: Whether we want to see the effects to the Disability population 0|1
    :param eng_very_well: Whether we want to see the effects to the population that speaks English very well 0|1
    :return: None, but saves the data to csv files
    """

    # Path to relevant folders
    pums_folder = data_dir + "ACS_PUMS/"
    state_folder = pums_folder + "state_data/"
    cross_walk_folder = data_dir + "GeoCorr/Public-use microdata area (PUMA)/"
    puma_equivalency = cross_walk_folder + "puma_equivalency.csv"
    current_data_folder = pums_folder + "Current_Eligibility/"
    covered_populations_folder = data_dir + "Covered_Populations/"

    # Dictionary to map the geography to the code column and crosswalk file
    geography_mapping = {
        "Public-use microdata area (PUMA)": ("puma22", "puma_equivalency.csv"),
        "118th Congress (2023-2024)": (
            "cd118", "United_States_Public-Use-Microdata-Area-(Puma)_to_118Th-Congress-(2023-2024).csv"),
        "State": ("state", "United_States_Public-Use-Microdata-Area-(Puma)_to_State.csv"),
        "County": ("county", "United_States_Public-Use-Microdata-Area-(Puma)_to_County.csv"),
        "ZIP/ZCTA": ("zcta", "United_States_Public-Use-Microdata-Area-(Puma)_to_ZIP-ZCTA.csv"),
        "Unified school district": (
            "sduni20", "United_States_Public-Use-Microdata-Area-(Puma)_to_Unified-School-District.csv"),
        "Metropolitan division": (
            "metdiv20", "United_States_Public-Use-Microdata-Area-(Puma)_to_Metropolitan-Division.csv")
    }

    # Get the code column and crosswalk file
    code_column, cw_name = geography_mapping[geography]

    # Set the crosswalk file
    cw_file = cross_walk_folder + cw_name

    # Create the columns for the dataframe
    columns = ["puma22", "Num Eligible", "Num Ineligible", "Percentage Eligible"]

    # Initialize the variables for the columns of covered populations
    covered_populations = [
        ("American Indian and Alaska Native", "aian"),
        ("Asian", "asian"),
        ("Black or African American", "black"),
        ("Native Hawaiian", "nhpi"),
        ("White", "white"),
        ("Hispanic or Latino", "hispanic"),
        ("Veteran", "veteran"),
        ("Elderly", "elderly"),
        ("DIS", "disability"),
        ("English less than very well", "eng_very_well")
    ]

    # Add the columns for the covered populations if they are used
    for population_name, population_var in covered_populations:
        if locals()[population_var] == 1:
            columns.append(population_name + " Eligible")

    # Create a dataframe to store the results, which will first be stored in puma
    main_df = pd.DataFrame(columns=columns)

    # Iterate through all folders in the State data folder
    for state in os.listdir(state_folder):
        # Save the path to the state folder
        state_files = state_folder + state + "/"
        # Iterate through all files in the state folder
        for file in os.listdir(state_files):
            if file.endswith(".zip"):
                # Delete the file
                os.remove(state_files + file)
            # Only read the csv files
            if file.endswith(".csv"):
                # Read the file
                temp_df = pd.read_csv(state_files + file, header=0)

                # Drop SERIALNO column
                temp_df = temp_df.drop(columns=["SERIALNO"])

                # Turn all acp_eligible values to 0
                temp_df["acp_eligible"] = 0

                # If the povpip is not 0, then use it as a criteria
                if povpip != 0:
                    # Turn all acp_eligible values to 1 if they meet the criteria and if we are using the criteria
                    temp_df.loc[(temp_df["POVPIP"] <= povpip) | ((temp_df["has_pap"] == 1) & (has_pap == 1)) |
                                ((temp_df["has_ssip"] == 1) & (has_ssip == 1)) |
                                ((temp_df["has_hins4"] == 1) & (has_hins4 == 1)) |
                                ((temp_df["has_snap"] == 1) & (has_snap == 1)), "acp_eligible"] = 1

                # If the povpip is 0, then use the other criteria
                else:
                    temp_df.loc[((temp_df["has_pap"] == 1) & (has_pap == 1)) |
                                ((temp_df["has_ssip"] == 1) & (has_ssip == 1)) |
                                ((temp_df["has_hins4"] == 1) & (has_hins4 == 1)) |
                                ((temp_df["has_snap"] == 1) & (has_snap == 1)), "acp_eligible"] = 1

                # Find the total number eligible for every PUMA_person
                unique_puma_person = temp_df["PUMA_person"].unique()

                # Iterate through every PUMA_person
                for puma_person in unique_puma_person:
                    # Create a dataframe for the PUMA_person
                    puma_df = temp_df.loc[temp_df["PUMA_person"] == puma_person]

                    # Find the number eligible and ineligible
                    eligible_df = puma_df.loc[puma_df["acp_eligible"] == 1]
                    ineligible_df = puma_df.loc[puma_df["acp_eligible"] == 0]

                    eligible = eligible_df["WGTP"].sum()
                    ineligible = ineligible_df["WGTP"].sum()

                    # Calculate the percentage eligible
                    percentage_eligible = eligible / (eligible + ineligible)

                    # Create a list to store the data
                    data = [puma_person, eligible, ineligible, percentage_eligible]

                    # If the covered populations are used, then add the number eligible for each population
                    for population_name, population_var in covered_populations:
                        if locals()[population_var] == 1:
                            data.append(eligible_df[population_name].sum())

                    # Add the puma_person and percentage eligible to the main dataframe
                    new_df = pd.DataFrame([data], columns=columns)

                    # If the main df is empty, set it equal to the new df
                    if main_df.empty:
                        main_df = new_df
                    else:
                        # Add the new dataframe to the main dataframe
                        main_df = pd.concat([main_df, new_df], axis=0)

    # Sort the main dataframe by puma22
    main_df.sort_values(by=["puma22"], inplace=True)

    # Round the percentage eligible column to two decimal places
    main_df["Percentage Eligible"] = (main_df["Percentage Eligible"] * 100).round(2)

    # PUMAs are seven digits, so add leading zeros
    main_df["puma22"] = main_df["puma22"].astype(str)
    main_df["puma22"] = main_df["puma22"].str.zfill(7)

    # If it is using 2010 PUMAs, then crosswalk the data to 2020 PUMAs
    if '0600102' in main_df['puma22'].values:
        main_df = crossWalkOldPumaNewPuma(main_df, puma_equivalency)

    # Create the file name
    file_name = "percentage_eligible"

    # Boolean to determine if there was a change to see the difference in eligibility
    add_col = False

    # If all the criteria are used, then do not add anything to the file name
    if povpip == 200 and has_pap == 1 and has_ssip == 1 and has_hins4 == 1 and has_snap == 1:
        file_name = "eligibility-by"
    # Else, add the criteria to the file name dynamically
    else:
        if povpip != 200:
            file_name = file_name + "_povpip_" + str(povpip)
        if has_pap == 1:
            file_name = file_name + "_has_pap"
        if has_ssip == 1:
            file_name = file_name + "_has_ssip"
        if has_hins4 == 1:
            file_name = file_name + "_has_hins4"
        if has_snap == 1:
            file_name = file_name + "_has_snap"
        add_col = True

    # Determine if any covered populations are used
    if (aian == 1 and asian == 1 and black == 1 and nhpi == 1 and white == 1 and hispanic == 1 and veteran == 1
            and elderly == 1 and disability == 1 and eng_very_well == 1):
        file_name += "-covered_populations"
    else:
        for population_name, population_var in covered_populations:
            if locals()[population_var] == 1:
                file_name += "_" + population_var

    # If the geography is PUMA, then do not crosswalk the data
    if code_column == "puma22":
        # If we are looking at changes, add the current percentage eligible column
        if add_col:
            # Read the original file
            if (aian == 1 or asian == 1 or black == 1 or nhpi == 1 or white == 1 or hispanic == 1 or veteran == 1 or
                    elderly == 1 or disability == 1 or eng_very_well == 1):
                original_file = current_data_folder + "eligibility-by-covered_populations-puma22.csv"
            else:
                original_file = current_data_folder + "eligibility-by-puma22.csv"
            original_df = pd.read_csv(original_file, header=0, dtype={"puma22": str})

            # Rename all the columns to have "Current" in front of them
            original_df = original_df.rename(columns={"Num Eligible": "Current Num Eligible",
                                                      "Num Ineligible": "Current Num Ineligible",
                                                      "Percentage Eligible": "Current Percentage Eligible"})

            # If covered populations are used, open that file and rename the columns
            if "covered_populations" in original_file:
                # Iterate through the covered populations
                for population_name, population_var in covered_populations:
                    if locals()[population_var] == 1:
                        original_df = original_df.rename(
                            columns={population_name + " Eligible": "Current " + population_name + " Eligible"})
                    else:
                        original_df = original_df.drop(columns=[population_name + " Eligible"])

            # Round the percentage eligible column to two decimal places
            main_df["Percentage Eligible"] = main_df["Percentage Eligible"].round(2)
            original_df["Current Percentage Eligible"] = original_df["Current Percentage Eligible"].round(2)

            # Reset the index
            original_df.reset_index(inplace=True)
            main_df.reset_index(inplace=True)
            original_df.drop(columns=["index"], inplace=True)
            main_df.drop(columns=["index"], inplace=True)

            # Add the current percentage eligible column to the main dataframe
            main_df = pd.concat([main_df, original_df], axis=1, join="outer", ignore_index=False, sort=True)

            # Calculate the difference between the two covered populations eligible columns
            for population_name, population_var in covered_populations:
                if locals()[population_var] == 1:
                    main_df["difference_" + population_var] = main_df[population_name + " Eligible"] - main_df[
                        "Current " + population_name + " Eligible"]
                    main_df["difference_percentage_" + population_var] = (main_df["difference_" + population_var] /
                                                                          main_df["Current " + population_name +
                                                                                  " Eligible"]) * 100
                    main_df = main_df.drop(
                        columns=["Current " + population_name + " Eligible", "difference_" + population_var])

            # Drop numeric columns that are no longer needed
            main_df = main_df.drop(columns=["Num Eligible", "Num Ineligible"])

            # Combine duplicate columns
            main_df = main_df.loc[:, ~main_df.columns.duplicated()]

            # Move the current percentage eligible column to the 2nd column
            columns = main_df.columns.tolist()

            # Remove the current percentage eligible column
            columns.remove("Current Percentage Eligible")

            # Add the current percentage eligible column to the second position
            columns.insert(1, "Current Percentage Eligible")

            # Remove the current num ineligible column
            columns.remove("Current Num Ineligible")

            # Add the current num ineligible column to the second position
            columns.insert(1, "Current Num Ineligible")

            # Remove the current num eligible column
            columns.remove("Current Num Eligible")

            # Add the current num eligible column to the second position
            columns.insert(1, "Current Num Eligible")

            # Reorder the columns
            main_df = main_df[columns]

            # Rename the "Percentage Eligible" column to "New Percentage Eligible"
            main_df = main_df.rename(columns={"Percentage Eligible": "New Percentage Eligible"})

        # Save the data
        file_name += "-puma22.csv"

        main_df = main_df.astype({code_column: str})
        main_df[code_column] = main_df[code_column].str.zfill(7)

        main_df = main_df.fillna(0)

        # Return the df and the file name
        return main_df, file_name

    # Else, crosswalk the data
    else:
        # Drop the percentage eligible column
        main_df = main_df.drop(columns=["Percentage Eligible"])

        # Read the crosswalk file
        dc, col_name = code_to_source_dict(cw_file, "puma")

        # Add the geography to the file name
        file_name += f"-{col_name}.csv"

        # Crosswalk the data
        new_df = crosswalkPUMAData(main_df, dc, "puma22", col_name)

        # Make the percentage eligible column
        new_df["New Percentage Eligible"] = new_df["Num Eligible"] / (new_df["Num Eligible"] + new_df["Num Ineligible"])

        # Round the percentage eligible column to two decimal places
        new_df["New Percentage Eligible"] = (new_df["New Percentage Eligible"] * 100).round(2)

        # If we are looking at changes, add the current percentage eligible column
        if add_col:
            # Read the original file
            if (aian == 1 or asian == 1 or black == 1 or nhpi == 1 or white == 1 or hispanic == 1 or veteran == 1 or
                    elderly == 1 or disability == 1 or eng_very_well == 1):
                original_file = current_data_folder + f"eligibility-by-covered_populations-{col_name}.csv"
            else:
                original_file = current_data_folder + f"eligibility-by-{col_name}.csv"
            original_df = pd.read_csv(original_file, header=0)

            # Rename all the columns to have "Current" in front of them
            original_df = original_df.rename(columns={"Num Eligible": "Current Num Eligible",
                                                      "Num Ineligible": "Current Num Ineligible",
                                                      "Percentage Eligible": "Current Percentage Eligible"})

            # If covered populations are used, open that file and rename the columns
            if "covered_populations" in original_file:
                # Iterate through the covered populations
                for population_name, population_var in covered_populations:
                    if locals()[population_var] == 1:
                        original_df = original_df.rename(
                            columns={population_name + " Eligible": "Current " + population_name + " Eligible"})
                    else:
                        original_df = original_df.drop(columns=[population_name + " Eligible"])

            # Round the percentage eligible column to two decimal places
            new_df["New Percentage Eligible"] = new_df["New Percentage Eligible"].round(2)
            original_df["Current Percentage Eligible"] = original_df["Current Percentage Eligible"].round(2)

            # Reset the index
            original_df.reset_index(inplace=True)
            new_df.reset_index(inplace=True)
            original_df.drop(columns=["index"], inplace=True)
            new_df.drop(columns=["index"], inplace=True)

            # Add the current percentage eligible column to the main dataframe
            new_df = pd.concat([new_df, original_df], axis=1, join="outer", ignore_index=False, sort=True)

            # Calculate the difference between the two covered populations eligible columns
            for population_name, population_var in covered_populations:
                if locals()[population_var] == 1:
                    new_df["difference_" + population_var] = new_df[population_name + " Eligible"] - new_df[
                        "Current " + population_name + " Eligible"]
                    new_df["difference_percentage_" + population_var] = ((new_df["difference_" + population_var] /
                                                                          new_df["Current " + population_name +
                                                                                 " Eligible"]) * 100).round(2)
                    new_df = new_df.drop(
                        columns=[population_name + " Eligible", "Current " + population_name + " Eligible"])

            # Drop numeric columns that are no longer needed
            new_df = new_df.drop(columns=["Num Eligible", "Num Ineligible"])

            # Combine duplicate columns
            new_df = new_df.loc[:, ~new_df.columns.duplicated()]

            # Move the current percentage eligible column to the second position
            columns = new_df.columns.tolist()

            # Remove the current percentage eligible column
            columns.remove("Current Percentage Eligible")

            # Add the current percentage eligible column to the second position
            columns.insert(1, "Current Percentage Eligible")

            # Remove the current percentage eligible column
            columns.remove("Current Num Ineligible")

            # Add the current percentage eligible column to the second position
            columns.insert(1, "Current Num Ineligible")

            # Remove the current percentage eligible column
            columns.remove("Current Num Eligible")

            # Add the current percentage eligible column to the second position
            columns.insert(1, "Current Num Eligible")

            # Reorder the columns
            new_df = new_df[columns]

            # If the code column is county, then add the rural column and county name column
            if code_column == "county":
                if "rural" not in new_df.columns.tolist():
                    # Download the covered population file
                    covered_pops_df = pd.read_csv(covered_populations_folder + "covered_populations.csv")

                    # Rename the columns
                    covered_pops_df = covered_pops_df.rename(columns={"geo_id": "county"})

                    # Turn the county column into a string and zero fill it
                    covered_pops_df["county"] = covered_pops_df["county"].astype(str)
                    covered_pops_df["county"] = covered_pops_df["county"].str.zfill(5)

                    new_df["county"] = new_df["county"].astype(str)
                    new_df["county"] = new_df["county"].str.zfill(5)

                    # Only keep county and rural columns
                    covered_pops_df = covered_pops_df[["county", "rural"]]

                    # Merge the dataframes
                    new_df = pd.merge(new_df, covered_pops_df, on="county", how="left")

                # Move the rural column to the second position
                columns = new_df.columns.tolist()

                # Move the rural column to the second position
                columns.remove("rural")

                # Add the rural column to the second position
                columns.insert(1, "rural")

                # Reorder the columns
                new_df = new_df[columns]
                if "CountyName" not in new_df.columns.tolist():
                    # Read the crosswalk file
                    df = pd.read_csv(cw_file, header=0, dtype={"county": str})

                    # Drop the duplicate county rows
                    df = df.drop_duplicates(subset=["county"])

                    df["county"] = df["county"].astype(str)
                    df["county"] = df["county"].str.zfill(5)

                    new_df["county"] = new_df["county"].astype(str)
                    new_df["county"] = new_df["county"].str.zfill(5)

                    # Add the "CountyName" column to the new dataframe
                    new_df = pd.merge(new_df, df[["county", "CountyName"]], on="county", how="left")

                # Move the CountyName column to the second position
                columns = new_df.columns.tolist()

                # Remove the CountyName column
                columns.remove("CountyName")

                # Add the CountyName column to the second position
                columns.insert(1, "CountyName")

                # Reorder the columns
                new_df = new_df[columns]

            # If the code column is metdiv, then add the metdiv name column
            if code_column == "metdiv20":
                if "MetDivName" not in new_df.columns.tolist():
                    # Read the crosswalk file
                    df = pd.read_csv(cw_file, header=0, dtype={"metdiv20": str})

                    # Drop the duplicate metdiv rows
                    df = df.drop_duplicates(subset=["metdiv20"])

                    df["metdiv20"] = df["metdiv20"].astype(str)
                    df["metdiv20"] = df["metdiv20"].str.zfill(5)

                    new_df["metdiv20"] = new_df["metdiv20"].astype(str)
                    new_df["metdiv20"] = new_df["metdiv20"].str.zfill(5)

                    # Add the "MetDivName" column to the new dataframe
                    new_df = pd.merge(new_df, df[["metdiv20", "MetDivName"]], on="metdiv20", how="left")

                # Move the MetDivName column to the second position
                columns = new_df.columns.tolist()

                # Remove the MetDivName column
                columns.remove("MetDivName")

                # Add the MetDivName column to the second position
                columns.insert(1, "MetDivName")

                # Reorder the columns
                new_df = new_df[columns]

        new_df = new_df.fillna(0)

        new_df = new_df.astype({code_column: str})

        if code_column == "state":
            new_df[code_column] = new_df[code_column].str.zfill(2)
        elif code_column == "county":
            new_df[code_column] = new_df[code_column].str.zfill(5)
        elif code_column == "zcta":
            new_df[code_column] = new_df[code_column].str.zfill(5)
        elif code_column == "metdiv20":
            new_df[code_column] = new_df[code_column].str.zfill(5)
        elif code_column == "cd118":
            new_df[code_column] = new_df[code_column].str.zfill(4)

        # Return the df and the file name
        return new_df, file_name
