##################################################
# • Data
# Being able to easily load and process data is a crucial task that can make any data science more pleasant. In this notebook, we will cover most common types often encountered in data science tasks.
##################################################

using BenchmarkTools
using DataFrames
using DelimitedFiles
using CSV
using XLSX
using Downloads

#######################################
# • 🗃️ Get some data
# In Julia, it's pretty easy to dowload a file from the web using the `download` (https://docs.julialang.org/en/v1/stdlib/Downloads/) function.

# Note: `download` depends on external tools such as curl, wget or fetch. So you must have one of these.
########################################

data_dir = "data/tables"

dir = Downloads.download("https://raw.githubusercontent.com/nassarhuda/easy_data/master/programming_languages.csv",
    "data/tables/programming_languages.csv")

data, header = readdlm(dir, ',', header=true)

header
data

# To write to a text file, you can:
writedlm("$data_dir/my_pl_dlm.txt", data, ';')


################################
# A more powerful package to use here is the `CSV` package. By default, the CSV package imports the data to a DataFrame, which can have several advantages as we will see below.

# In general,[`CSV.jl`](https://juliadata.github.io/CSV.jl/stable/) is the recommended way to load CSVs in Julia. Only use `DelimitedFiles` when you have a more complicated file where you want to specify several things.
################################

data2 = CSV.read(dir, DataFrame)

typeof(data)
data[1:10,:]

typeof(data2)
data2[1:10,:]
data2[!,:year]
data2.year
data2.language

names(data2)
describe(data2)

@btime data,header = readdlm(dir,',', header=true);
@btime data2 = CSV.read(dir, DataFrame);

# To write to a *.csv file using the CSV package
CSV.write("$data_dir/pl_CSV.csv", DataFrame(data, :auto))

# Another type of files that we may often need to read is `XLSX` files. Let's try to read a new file.

table1 = XLSX.readdata(
    "$data_dir/zillow_data_download_april2020.xlsx", #file name
    "Sale_counts_city", #sheet name
    "A1:F9" #cell range
)

# If you don't want to specify cell ranges... though this will take a little longer...

table2 = DataFrame(XLSX.readtable(
    "$data_dir/zillow_data_download_april2020.xlsx","Sale_counts_city"
))

typeof(table2)
table2[1:1,:]
table2.RegionID

foods = ["apple", "cucumber", "tomato", "banana"]
calories = [105, 47, 22, 105]
prices = [0.85, 1.6, 0.8, 0.6]
dataframe_calories = DataFrame(item=foods,calories=calories)
dataframe_prices = DataFrame(item=foods,price=prices)

dataframe = innerjoin(dataframe_calories,dataframe_prices,on=:item)

# we can also use the DataFrame constructor on a Matrix
DataFrame(table1, :auto)

# You can also easily write data to an XLSX file
# if you already have a dataframe: 
XLSX.writetable("$data_dir/my_xlsx.xlsx", collect(DataFrames.eachcol(dataframe)), DataFrames.names(dataframe))

XLSX.writetable("writefile_using_XLSX.xlsx",G[1],G[2])

####################################
# ⬇️ Importing your data
#
# Often, the data you want to import is not stored in plain text, and you might want to import different kinds of types. Here we will go over importing `jld`, `npz`, `rda`, and `mat` files. Hopefully, these four will capture the types from four common programming languages used in Data Science (Julia, Python, R, Matlab).

# We will use a toy example here of a very small matrix. But the same syntax will hold for bigger files.
#
# ```
# 4×5 Array{Int64,2}:
#  2  1446  1705  1795  1890
#  3  2926  3121  3220  3405
#  4  2910  3022  2937  3224
#  5  1479  1529  1582  1761
#  ```
######################################

import_dir = "data/imports" 

using JLD
jld_data = JLD.load("$import_dir/tempdata.jld")
save("$import_dir/mywrite.jld", "A", jld_data)


using NPZ
npz_data = npzread("$import_dir/tempdata.npz")
npzwrite("$import_dir/mywrite.npz", npz_data)


using RData
R_data = RData.load("$import_dir/tempdata.rda")
#Need Install R
using RCall
@rput R_data
R"save(R_data, file=\"mywrite.rda\")"


using MAT
Matlab_data = matread("$import_dir/tempdata.mat")
matwrite("$import_dir/mywrite.mat",Matlab_data)


typeof(jld_data)
typeof(npz_data)
typeof(R_data)
typeof(Matlab_data)


#####################################################
# 🔢 Time to process the data from Julia
# We will mainly cover `Matrix` (or `Vector`), `DataFrame`s, and `dict`s (or dictionaries). Let's bring back our programming languages dataset and start playing it the matrix it's stored in.
#
# Here are some quick questions we might want to ask about this simple data.
# - Which year was was a given language invented?
# - How many languages were created in a given year?
#####################################################

# Q1: Which year was was a given language invented?
function year_created(data, language::String)
    loc = findfirst(data[:,2] .== language)
    return data[loc,1]
end

year_created(data, "Julia")
year_created(data, "W")

function year_created_handle_error(data, language::String)
    loc = findfirst(data[:,2] .== language)

    # ternary error
    isnothing(loc) ?
        error("Error: Language not found.") :
        return data[loc,1]

    # another way to check:
    !isnothing(loc) && return P[loc,1]
    error("Error: Language not found.")
end

year_created_handle_error(data,"W")

# Q2: How many languages were created in a given year?
function how_many_per_year(data, year::Int64)
    year_count = length(findall(data[:,1] .== year))
    return year_count
end

how_many_per_year(data, 2011)

# Now let's try to store this data in a DataFrame...
# DataFrame(year = data[:,1], language = data[:,2]) # or DataFrame(data)
data_df = DataFrame(data, :auto)
# but we already have done this with data2
data_df = data2

# Even better, since we know the types of each column, we can create the DataFrame as follows:
data_df = DataFrame(year = Int.(data[:,1]), language = string.(data[:,2]))

# And now let's answer the same questions we just answered...

# Q1: Which year was was a given language invented?
# it's a little more intuitive and you don't need to remember the column ids
function year_created(data_df,language::String)
    loc = findfirst(data_df.language .== language)
    return data_df.year[loc]
end

year_created(data_df,"Julia")
year_created(data_df,"W")

function year_created_handle_error(data_df,language::String)
    loc = findfirst(data_df.language .== language)
    !isnothing(loc) && return data_df.year[loc]
    error("Error: Language not found.")
end

year_created_handle_error(data_df,"W")

# Q2: How many languages were created in a given year?
function how_many_per_year(data_df,year::Int64)
    year_count = length(findall(data_df.year.==year))
    return year_count
end

how_many_per_year(data_df,2011)

# Next, we'll use dictionaries. A quick way to create a dictionary is with the `Dict()` command. But this creates a dictionary without types. Here, we will specify the types of this dictionary.

# A quick example to show how to build a dictionary
d1 = Dict([("A", 1), ("B", 2),(1,[1,2])])
d2 = Dict("A"=>1, "B"=>2, 1=>[1,2])
d1 == d2

data_dictionary = Dict{Integer,Vector{String}}()
data_dictionary[67] = ["julia","programming"]

# this is not going to work.
data_dictionary["julia"] = 7


# Now, let's populate the dictionary with years as keys and vectors that hold all the programming languages created in each year as their values. Even though this looks like more work, we often need to do it just once.


dict = Dict{Integer,Vector{String}}()

for i = 1:size(data,1)
    year,lang = data[i,:]
    if year in keys(dict)
        dict[year] = push!(dict[year],lang) 
        # note that push! is not our favorite thing to do in Julia, 
        # but we're focusing on correctness rather than speed here
    else
        dict[year] = [lang]
    end
end

dict

# Though a smarter way to do this is:
curyear = data_df.year[1]
data_dictionary[curyear] = [data_df.language[1]]

for (i,nextyear) in enumerate(data_df.year[2:end])
    if nextyear == curyear #same key
        data_dictionary[curyear] = push!(data_dictionary[curyear],data_df.language[i+1])
    else
        curyear = nextyear
        data_dictionary[curyear] = [data_df.language[i+1]]
    end
end

data_dictionary
delete!(data_dictionary, 67)

length(keys(dict))
length(keys(data_dictionary))
length(unique(data[:,1]))


# Q1: Which year was was a given language invented?
# now instead of looking in one long vector, we will look in many small vectors
function year_created(data_dictionary,language::String)
    keys_vec = collect(keys(data_dictionary))
    lookup = map(keyid -> findfirst(data_dictionary[keyid].==language),keys_vec)
    # now the lookup vector has `nothing` or a numeric value. We want to find the index of the numeric value.
    return keys_vec[findfirst((!isnothing).(lookup))]
end

year_created(data_dictionary,"Julia")
year_created(data_dictionary,"W")

# Q2: How many languages were created in a given year?
how_many_per_year(data_dictionary,year::Int64) = length(data_dictionary[year])
how_many_per_year(data_dictionary,2011)


#####################################
# 📝 A note about missing data
#####################################

# assume there were missing values in our dataframe
data[1,1] = missing
data_df = DataFrame(year = data[:,1], language = data[:,2])

dropmissing(data_df)