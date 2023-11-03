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
        "Public-use microdata area (PUMA)": ("puma22", ""),
        "118th Congress (2023-2024)": ("cd118", ""),
        "State": ("state", ""),
        "County": ("county", "United_States_Public-Use-Microdata-Area-(Puma)_to_County.csv"),
        "Metropolitan division":
            ("metdiv20", "United_States_Public-Use-Microdata-Area-(Puma)_to_Metropolitan-Division.csv"),
        "ZIP/ZCTA": ("zcta", "United_States_Public-Use-Microdata-Area-(Puma)_to_Zip-Zcta.csv")
    }
    geo_col = geography_mapping[geography][0]
    cw_file = cross_walk_folder + geography_mapping[geography][1]

    # Get the code column and crosswalk file
    if geo_col == "zcta":
        code_column, cw_name = geography_mapping[geography]

        # Set the crosswalk file
        cw_file = cross_walk_folder + cw_name

    if geo_col == "zcta":
        columns = ["puma22", "Num Eligible", "Num Ineligible", "Percentage Eligible"]
    else:
        columns = [geo_col, "Num Eligible", "Num Ineligible", "Percentage Eligible"]

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
            columns.append(population_name + " Total")

    # Create the file name
    file_name = "households_eligible"

    # Boolean to determine if there was a change to see the difference in eligibility
    add_col = False
    final_column = ""

    # If all the criteria are used, then do not add anything to the file name
    if povpip == 200 and has_pap == 1 and has_ssip == 1 and has_hins4 == 1 and has_snap == 1:
        file_name = "eligibility-by"
    # Else, add the criteria to the file name dynamically
    else:
        if povpip != 0:
            file_name += "_povpip_" + str(povpip)
            final_column = "povpip_" + str(povpip)
        if has_pap == 1:
            file_name += "_has_pap"
            final_column += "_has_pap"
        if has_ssip == 1:
            file_name += "_has_ssip"
            final_column += "_has_ssip"
        if has_hins4 == 1:
            file_name += "_has_hins4"
            final_column += "_has_hins4"
        if has_snap == 1:
            file_name += "_has_snap"
            final_column += "_has_snap"
        add_col = True

    # Determine if any covered populations are used
    if (aian == 1 and asian == 1 and black == 1 and nhpi == 1 and white == 1 and hispanic == 1 and veteran == 1
            and elderly == 1 and disability == 1 and eng_very_well == 1):
        file_name += "-covered_populations"
    else:
        for population_name, population_var in covered_populations:
            if locals()[population_var] == 1:
                file_name += "_" + population_var

    file_name += f"-{geo_col}.csv"

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
            if file.endswith(f"{geo_col}.csv") and geo_col != "zcta":
                # Read the file
                temp_df = pd.read_csv(state_files + file, header=0)

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
                unique_puma_person = temp_df[geo_col].unique()

                # Iterate through every PUMA_person
                for code in unique_puma_person:
                    # Create a dataframe for the PUMA_person
                    puma_df = temp_df.loc[temp_df[geo_col] == code]

                    # Find the number eligible and ineligible
                    eligible_df = puma_df.loc[puma_df["acp_eligible"] == 1]
                    ineligible_df = puma_df.loc[puma_df["acp_eligible"] == 0]

                    eligible = eligible_df["WGTP"].sum()
                    ineligible = ineligible_df["WGTP"].sum()

                    # Calculate the percentage eligible
                    percentage_eligible = ((eligible / (eligible + ineligible)) * 100).round(2)

                    # Create a list to store the data
                    data = [code, eligible, ineligible, percentage_eligible]

                    # If the covered populations are used, then add the number eligible for each population
                    for population_name, population_var in covered_populations:
                        if locals()[population_var] == 1:
                            data.append(eligible_df[population_name].sum())
                            data.append(puma_df[population_name].sum())

                    # Add the puma_person and percentage eligible to the main dataframe
                    new_df = pd.DataFrame([data], columns=columns)

                    # If the main df is empty, set it equal to the new df
                    if main_df.empty:
                        main_df = new_df
                    else:
                        # Add the new dataframe to the main dataframe
                        main_df = pd.concat([main_df, new_df], axis=0)


            elif geo_col == "zcta" and file.endswith("puma22.csv"):
                # Read the file
                temp_df = pd.read_csv(state_files + file, header=0)

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
                unique_puma_person = temp_df["puma22"].unique()

                # Iterate through every PUMA_person
                for code in unique_puma_person:
                    # Create a dataframe for the PUMA_person
                    puma_df = temp_df.loc[temp_df["puma22"] == code]

                    # Find the number eligible and ineligible
                    eligible_df = puma_df.loc[puma_df["acp_eligible"] == 1]
                    ineligible_df = puma_df.loc[puma_df["acp_eligible"] == 0]

                    eligible = eligible_df["WGTP"].sum()
                    ineligible = ineligible_df["WGTP"].sum()

                    # Calculate the percentage eligible
                    percentage_eligible = ((eligible / (eligible + ineligible)) * 100).round(2)

                    # Create a list to store the data
                    data = [code, eligible, ineligible, percentage_eligible]

                    # If the covered populations are used, then add the number eligible for each population
                    for population_name, population_var in covered_populations:
                        if locals()[population_var] == 1:
                            data.append(eligible_df[population_name].sum())
                            data.append(puma_df[population_name].sum())

                    # Add the puma_person and percentage eligible to the main dataframe
                    new_df = pd.DataFrame([data], columns=columns)

                    # If the main df is empty, set it equal to the new df
                    if main_df.empty:
                        main_df = new_df
                    else:
                        # Add the new dataframe to the main dataframe
                        main_df = pd.concat([main_df, new_df], axis=0)

    if geo_col != "zcta":

        main_df = main_df.drop(columns=["Percentage Eligible"])

        # Combine the rows with the same code by adding every column
        main_df = main_df.groupby(geo_col).sum()

        # Reset the index
        main_df.reset_index(inplace=True)

        # Calculate the percentage eligible
        main_df["Percentage Eligible"] = ((main_df["Num Eligible"] / (main_df["Num Eligible"] +
                                                                      main_df["Num Ineligible"])) * 100).round(2)

        main_df.sort_values(by=[geo_col], inplace=True)

        if geo_col == "puma22":
            main_df["puma22"] = main_df["puma22"].astype(str).str.zfill(7)
        elif geo_col == "state":
            main_df["state"] = main_df["state"].astype(str).str.zfill(2)
        elif geo_col == "county" or geo_col == "metdiv20":
            main_df[geo_col] = main_df[geo_col].astype(str).str.zfill(5)
        elif geo_col == "cd118":
            main_df["cd118"] = main_df["cd118"].astype(str).str.zfill(4)

        if add_col:
            # Read the original file
            if (aian == 1 or asian == 1 or black == 1 or nhpi == 1 or white == 1 or hispanic == 1 or veteran == 1 or
                    elderly == 1 or disability == 1 or eng_very_well == 1):
                original_file = current_data_folder + f"eligibility-by-covered_populations-{geo_col}.csv"
            else:
                original_file = current_data_folder + f"eligibility-by-{geo_col}.csv"
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
                    main_df = main_df.rename(
                        columns={population_name + " Eligible": f"{final_column} {population_name} Eligible"})

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
            main_df = main_df.rename(columns={"Percentage Eligible": "New Percentage Eligible",
                                              "Num Eligible": f"{final_column} Num Eligible",
                                              "Num Ineligible": f"{final_column} Num Ineligible"})

            # Drop new percentage eligible column
            main_df = main_df.drop(columns=["New Percentage Eligible", "Current Percentage Eligible"])

            main_df = main_df.fillna(0)

        if geo_col == "county":
            if "rural" not in main_df.columns:
                # Download the covered population file
                covered_pops_df = pd.read_csv(covered_populations_folder + "covered_populations.csv")

                # Rename the columns
                covered_pops_df = covered_pops_df.rename(columns={"geo_id": "county"})

                # Turn the county column into a string and zero fill it
                covered_pops_df["county"] = covered_pops_df["county"].astype(str)
                covered_pops_df["county"] = covered_pops_df["county"].str.zfill(5)

                main_df["county"] = main_df["county"].astype(str)
                main_df["county"] = main_df["county"].str.zfill(5)

                # Only keep county and rural columns
                covered_pops_df = covered_pops_df[["county", "rural"]]

                # Merge the dataframes
                main_df = pd.merge(main_df, covered_pops_df, on="county", how="left")

            # Move the rural column to the second position
            columns = main_df.columns.tolist()

            # Move the rural column to the second position
            columns.remove("rural")

            # Add the rural column to the second position
            columns.insert(1, "rural")

            # Reorder the columns
            main_df = main_df[columns]
            if "CountyName" not in main_df.columns.tolist():
                # Read the crosswalk file
                df = pd.read_csv(cw_file, header=0, dtype={"county": str})

                # Drop the duplicate county rows
                df = df.drop_duplicates(subset=["county"])

                df["county"] = df["county"].astype(str)
                df["county"] = df["county"].str.zfill(5)

                main_df["county"] = main_df["county"].astype(str)
                main_df["county"] = main_df["county"].str.zfill(5)

                # Add the "CountyName" column to the new dataframe
                main_df = pd.merge(main_df, df[["county", "CountyName"]], on="county", how="left")

            # Move the CountyName column to the second position
            columns = main_df.columns.tolist()

            # Remove the CountyName column
            columns.remove("CountyName")

            # Add the CountyName column to the second position
            columns.insert(1, "CountyName")

            # Reorder the columns
            main_df = main_df[columns]

        # If the code column is metdiv, then add the metdiv name column
        if geo_col == "metdiv20":
            if "MetDivName" not in main_df.columns.tolist():
                # Read the crosswalk file
                df = pd.read_csv(cw_file, header=0, dtype={"metdiv20": str})

                # Drop the duplicate metdiv rows
                df = df.drop_duplicates(subset=["metdiv20"])

                df["metdiv20"] = df["metdiv20"].astype(str)
                df["metdiv20"] = df["metdiv20"].str.zfill(5)

                main_df["metdiv20"] = main_df["metdiv20"].astype(str)
                main_df["metdiv20"] = main_df["metdiv20"].str.zfill(5)

                # Add the "MetDivName" column to the new dataframe
                main_df = pd.merge(main_df, df[["metdiv20", "MetDivName"]], on="metdiv20", how="left")

            # Move the MetDivName column to the second position
            columns = main_df.columns.tolist()

            # Remove the MetDivName column
            columns.remove("MetDivName")

            # Add the MetDivName column to the second position
            columns.insert(1, "MetDivName")

            # Reorder the columns
            main_df = main_df[columns]

        if "Percentage Eligible" in main_df.columns:
            main_df = main_df.drop(columns=["Percentage Eligible"])

        return main_df, file_name

    else:
        main_df = main_df.drop(columns=["Percentage Eligible"])

        # Combine the rows with the same code by adding every column
        main_df = main_df.groupby("puma22").sum()

        # Reset the index
        main_df.reset_index(inplace=True)

        # Calculate the percentage eligible
        main_df["Percentage Eligible"] = ((main_df["Num Eligible"] / (main_df["Num Eligible"] +
                                                                      main_df["Num Ineligible"])) * 100).round(2)

        main_df["puma22"] = main_df["puma22"].astype(str).str.zfill(7)

        main_df.sort_values(by=["puma22"], inplace=True)

        # Drop the percentage eligible column
        main_df = main_df.drop(columns=["Percentage Eligible"])

        # Read the crosswalk file
        dc, col_name = code_to_source_dict(cw_file, "puma")

        # Crosswalk the data
        new_df = crosswalkPUMAData(main_df, dc, "puma22", col_name)

        new_df = new_df.groupby("zcta").sum()

        new_df.reset_index(inplace=True)

        # Make the percentage eligible column
        new_df["Percentage Eligible"] = new_df["Num Eligible"] / (new_df["Num Eligible"] + new_df["Num Ineligible"])

        # Round the percentage eligible column to two decimal places
        new_df["Percentage Eligible"] = (new_df["Percentage Eligible"] * 100).round(2)

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
                    new_df = new_df.rename(
                        columns={population_name + " Eligible": f"{final_column} {population_name} Eligible"})

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

            # Rename the columns
            new_df = new_df.rename(columns={"Num Eligible": f"{final_column} Num Eligible",
                                            "Num Ineligible": f"{final_column} Num Ineligible"})

            new_df = new_df.drop(columns=["Current Percentage Eligible"])

        # Drop new percentage eligible column
        new_df = new_df.drop(columns=["Percentage Eligible"])

        new_df = new_df.fillna(0)

        new_df["zcta"] = new_df["zcta"].astype(str).str.zfill(5)

        return new_df, file_name
