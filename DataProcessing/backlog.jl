#load tweets and select only ones of english language
df = DataFrame!(CSV.File("df_final_dates.csv"))
df_random = df[shuffle(axes(df,1)),:]
df_random = df_random[1:214104,:]

df_full = vcat(df_full,df_random)


df_en = filter(x -> x.Language == "en", df)
CSV.write("df_en.csv", df_en)
#get tweet IDs and save them to a new csv
df_ids = DataFrame()
df_ids."Ids" = df_random.Id
CSV.write("df_ids.csv", df_ids)
#for most RTet
df_ids_RTs = DataFrame()
df_ids_RTs.id = c.Id
CSV.write("df_ids_RTs.csv", df_ids_RTs)


create_sysimage([:Plots,:CSV,:DataFrames,:LightGraphs,:MetaGraphs, :GraphPlot,:Plots,:StatsPlots], sysimage_path="sys_plots.so", precompile_execution_file="precompile.jl")

#confirm that the last row of the tweets_RT corresponds to the last row of the RT dataframe
res = [i for i in eachrow(c) if i.Id == 1323457576927850496]


name_dict, text_dict = create_RT_csv(df_alternating,words)
using DataFramesMeta
a = @where(df_en, :full_text != "")
a = filter(x -> (x.:full_text != "",df_en)
dropmissing!(df_en)
a = @where(df_en, :Id .> in(:Id,df_ids))

#piecing together the final DF
CSV.write("Data\\V3\\df_final.csv",df_full)

df_full = DataFrame!()
df_9_10 = DataFrame(CSV.File("Data\\V3\\df_7.csv";threaded=false))
df_full = vcat(df_full,df_9_10)
for i in 7:8
    println(i)
    df_temp = DataFrame(CSV.File("Data\\V3\\df_8.csv"))
    df_full = vcat(df_full,df_temp)
end
unique!(df_full)
file = open("df_1.csv")
cleanfunction(string) = replace(string,"\"\"" =>"\"")
cleaned_file = IOBuffer(cleanfunction(read(file,String)))
test=CSV.read(cleaned_file,DataFrame)

haskey(dict_2,1278368973948694528)
function tweet_dict_f(words)
    tweet_dict = Array{Any}(undef,0)

    @simd for value in words
        push!(tweet_dict,JSON.parse(value))
    end
    return tweet_dict
end

#load tweets from JSON
words = readlines("Data\\V3\\json.jsonl", enc"UTF-16")
#build several dicts from the content
dict = tweet_dict_f(words[700001:end])
dict_id = [x["id"] for x in dict]
dict_text = [x["full_text"] for x in dict]
dict_name = [x["user"]["name"] for x in dict]
dict_2 = Dict(zip(dict_id,dict_text))
dict_3 = Dict(zip(dict_id,dict_name))
#filter the big df for the ids of the tweets
small_df = @where(df_random, in.(:Id,[keys(dict_2)]))
insertcols!(small_df,2, :FullText => ["" for i in nrow(small_df)])
insertcols!(small_df,3, :ScreenName => ["" for i in nrow(small_df)])
#and bring that back together here
small_df = @eachrow small_df begin
    :FullText = dict_2[:Id]
    :ScreenName = dict_3[:Id]
end
CSV.write("Data\\V3\\df_8.csv",small_df)

#take last ID from words, delete until then in df_ids, then do twarc again
#loading dfs
#df_alternating =  DataFrame!(CSV.File("df_alternating_new.csv"))
df_en_matched =  DataFrame!(CSV.File("df_en_full_text.csv"))
const df_en_const = df_en[1:1000000,:]
a = alternating_mixing(df_en)

#adjust column names so we have symbols
insertcols!(df_full,1, :Created => [Date(2013) for i in 1:nrow(df_full)])
insertcols!(df_full,3, :CreatedAt1 => [df_full[i,"CreatedAt"] for i in 1:nrow(df_full)])
select!(hashtag_df,Not(:CreatedAt))
#parse the dates so we can sort them
allowmissing!(df_full)
@simd for row in eachrow(df_full)
    try
        row.:Created2 = Date(match(r"^[^\s]+",row.:Created).match,"m/d/y")
    catch
        if typeof(row.:Created) == Date
            row.:Created2 = row.:Created
        else
            row.:Created2 = missing
        end
    end
end

df_full[1:end,:Created] = df_full[1:end,:Created2]
#filter out missing dates
filter!(row->!ismissing(row.:Created), df_random)
sort!(df_full,(:Created))
#and drop old dates
select!(df_full,Not(:Created2))
df_full = DataFrame!(CSV.File("Data\\V3\\df_even_dates.csv"))
dropmissing(df_full)

CSV.write("Data\\V3\\df_even_dates.csv",df_full)
CSV.write("Data\\V3\\df_even_dates_ids.csv",df_ids)

dates = [Date(match(r"^[^\s]+",hashtag_df[i,:CreatedAt]).match,"m/d/y") for i in 1:nrow(hashtag_df)]

#old df loading
df_random = DataFrame!(CSV.File("Data\\V3\\df_even_dates.csv"))
df_random = df_en[shuffle(axes(df_en,1)),:]
df_random = df_random[1:10000000,:]

a = alternating_mixing(df_random)
df_random = a

#convert row of df to int
df_en[!, "From-User-Id"] = Int.(parse.(Float64, df_en[:, "From-User-Id"]))

#checking the df for trump content
dropmissing!(df_en)
filter!(x-> !ismissing(x."From-User-Id"), df_en)
#tweets from trump
println(filter(x-> Int(x."From-User-Id") ==   25073877, df_en))
#tweets from screen name trump
println(filter(x-> x."ScreenName" == "Donald J. Trump", df_en))
#tweets with wrong trump id
a = filter(x-> Int(x."From-User-Id") ==   1108353072747692032, df_en)
println(Int(a[1,"To-User-Id"]))
#get trump status
println(filter(x-> Int(x."From-User-Id") ==  1208937404322795523, df_en))
println(size(filter(x-> x."To-User-Id" != -1, df_en)))
