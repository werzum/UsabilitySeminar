#TODO: IDEAS for analyzing
#use of hashtag and negativity/positivity
#most active users (most from)-> what did they spread?
#most influential users (most RTs) -> what did they spread?
#network with most RTetd content
#color positivity/negativity in responses


#filter for bots with CAP score higher 0.95
df_hashtag = @where(df_en, :univ_raw_score_overall .> 0.9)
#hashtag use
hashtag_df = @where(df_hashtag, occursin.("#",:FullText))
hashtag_df = sort(hashtag_df,:Created)
#add column with only hashtag
insertcols!(hashtag_df,4,:Hashtag => [[] for i in nrow(hashtag_df)])
#regex the full_text so that only hashtags remain
for row in eachrow(hashtag_df)
    matches = collect(eachmatch(r"(#[^\s]+)", row.:FullText))
    row.:Hashtag = (x->String(x.match)).(matches)
end

function top_hashtags(df)
    a = df[1:end,:Hashtag]
    #splat all hashtags to a counter
    dict = Dict(counter(vcat(a...)))

    #and find the top 3 entries
    top1 = findmax(dict)
    delete!(dict, top1[2])
    top2 = findmax(dict)
    delete!(dict, top2[2])
    top3 = findmax(dict)

    return [top1,top2,top3]
end
#generate toptags
toptags = for_x_days(7,hashtag_df,top_hashtags)
#get the top hashtags
top1count = [x[1][1] for x in toptags]
top1tags = [x[1][2] for x in toptags]
#create a date array for better x-axis
#which can sadly apparently not be used simultaneously with the annotations - sad!
dates = [df_en[1,"Created"]+Dates.Day(7*i) for i in 0:19]
plot(top1count,xlabel="Time in weeks",ylabel="Amount of Hashtag mentions",label="",size=(800,500))
#generate the annotations
anno = [(i, top1count[i], Plots.text(top1tags[i],8)) for i in 1:length(toptags)]
annotate!(anno)

# #do it again with second highest tags
# top2count = [x[2][1] for x in toptags]
# top2tags = [x[2][2] for x in toptags]
# plot!(top2count)
# anno = [(i, top2count[i], text(top2tags[i],8)) for i in 1:length(toptags)]
# annotate!(anno)

#do it again for the full df
temp = top_hashtags(hashtag_df)

#sort by RTs
df_rts = sort(df_en, (:"Retweet-Count"))
df_rts = df_rts[end-10:end,:]
#drop columns so we have relevant
select!(df_rts,["CreatedAt","FullText","ScreenName","Retweet-Count"])
#so retweet score counts how often the retweeted tweet has been RTed, not this one - therefore we get only randos there

#get active df
#in data_processing
#and select rows of 10 top active with their content
select!(df_active,["CreatedAt","FullText","ScreenName","Retweet-Count"])
print(df_active)
#and get the latex output
print(latexify(df_active, latex=false, env=:table))
#now for the same df but with 1000 most important

#get old df to update to-user-id
df_en1 = DataFrame!(CSV.File("Data\\V1\\df_en.csv"))
dict_en1 = Dictionary(df_en1[:,"Id"],df_en1[:,"To-User-Id"])
for row in eachrow(df_en)
    try
        row."To-User-Id" = dict_en1[row."Id"]
    catch
        row."To-User-Id" = -1
    end
end

graph = create_graph(df_active)
plot_graph(graph;labels=false)
