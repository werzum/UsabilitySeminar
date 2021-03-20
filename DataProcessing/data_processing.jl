#a nice function that scales input
function scale(min_m,max_m,min_t,max_t,m)
    return (m-min_m)/(max_m-min_m)*(max_t-min_t)+min_t
end

function alternating_mixing(df_en)
    #select the 1/3 most retweeted messages
    df_rts = sort(df_en, (:"Retweet-Count"))
    df_rts = df_rts[end-1000:end,:]

    #get randomly selected tweets not in the retweet dataframe
    df_random = df_en[shuffle(axes(df_en,1)),:]
    # #select 1ml randomly to speed things up
    # df_random = df_random[1:1000000,:]
    # rows = eachrow(df_rts)
    # df_random = filter(row->!in(row."From-User-Id",rows),df_random)
    df_random = df_random[1:333333,:]

    #generate a dict to rapidly count the number of tweets from each user
    unique_ids_from = Set(unique(df_en."From-User-Id"))
    a = zip(unique_ids_from,Array{Int}(undef,length(unique_ids_from)))
    tweets_from_dict = Dict(a)
    #and then count the tweets in the df
    @simd for row in eachrow(df_en)
        tweets_from_dict[row."From-User-Id"] += 1
    end

    #create a array so we can sort this (out)
    keys = Array{Int}(undef,0)
    vals = Array{Int}(undef,0)
    #extract the keys to sort them
    for (index,val) in enumerate(tweets_from_dict)
        if(!ismissing(val.first))
            if(val.first)!=0
                    push!(keys, val.first)
                    push!(vals, val.second)
            end
        end
    end
    result = hcat(keys,vals)
    sort!(result;dims=1)

    #and now draw tweets from the dataframe
    #select tweets from the 200.000 most active users
    result_active = result[end-10000:end,1]
    # rows = eachrow(result_active)
    df_active = filter(row-> !ismissing(row."From-User-Id") && in(row."From-User-Id",result_active),df_en)
    # #filter the duplicates from the random and retweet dataframe
    # rows = eachrow(df_rts)
    # df_active = filter(row->!in(row."From-User-Id",rows),df_active)
    # rows = eachrow(df_random)
    # df_active = filter(row->!in(row."From-User-Id",rows),df_active)
    print(nrow(df_active))
    #and the reduce this so we remain with 3330000 tweets
    df_active = df_active[shuffle(axes(df_active,1)),:]
    df_active = df_active[1:333333,:]

    #merge the dataframes and eliminate dupes
    df_return = vcat(df_rts,df_random,df_active)
    #and return the df
    return df_return
end

function create_RT_csv(df,words)

    #create dict with the texts
    tweet_ID_text_dict = Dict()
    tweet_ID_name_dict = Dict()
    for elm in words
        elm = JSON.parse(elm)
        tweet_ID_text_dict[elm["id"]] = elm["full_text"]
        tweet_ID_name_dict[elm["in_reply_to_user_id_str"]] = elm["in_reply_to_screen_name"]
    end
    #build a dict of the id - screen names
    #and add it to the RT df
    insertcols!(df_en,2, "full_text" => ["" for i in nrow(df_en)])
    insertcols!(df_en,3, "screen_name" => ["" for i in nrow(df_en)])
    for row in eachrow(df)
        try
            row."full_text" = tweet_ID_text_dict[row.Id]
        catch
        end
        try
            row."screen_name" = tweet_ID_name_dict[string(row."From-User-Id")]
        catch
        end
    end
    #save the DF
    # CSV.write("df_en_full_text.csv",a)
    return tweet_ID_name_dict,tweet_ID_text_dict
end

dropmissing!(df_en)
function create_graph(df_en,nodenumber)
    meta_graph = MetaGraph(SimpleGraph())

    #df_en = @where(df_en, :user_screen_name != Missing)
    strip(a) = replace(a, r"[^\x20-\x7e]" => "")
    for row in eachrow(df_en)
        try
            row."ScreenName" = strip(row."ScreenName")
        catch
        end
    end
    #df_en[!, "user_screen_name"] = convert.(String, df_en[:, "user_screen_name"])
    #1. generate set of user ids and the corresonding names
    #get unique IDs from the DF, add those vertices to the graph and give it the respective ID
    unique_ids_from = Int64.(Set(unique(df_en."From-User-Id")))
    unique_ids_to = Int64.(Set(unique(df_en."To-User-Id")))
    unique_ids = collect(union(unique_ids_to,unique_ids_from))

    #2.create dict of names and their index
    names = unique(df_en[1:nodenumber,"ScreenName"])
    indexarr = [1:length(names)...]
    name_dict = Dict(zip(names,indexarr))
    nodelabels = names
    #and create dict of names and their id and of their bot score
    id_dict = Dict()
    bot_dict = Dict()
    @simd for row in eachrow(df_en)
            id_dict[row."From-User-Id"] = row."ScreenName"
            bot_dict[row."ScreenName"] = row."en_cap"
    end

    #3. add #nr unique names to graph
    add_vertices!(meta_graph, length(nodelabels))

    #4. generate a dict with the user_screen_name and the number of retweets
    rt_dict = Dict(zip(collect(df_en."ScreenName"),zeros(nrow(df_en))))
    #5. and iterate over the df to count the retweets
    @simd for row in eachrow(df_en)
        if !occursin("RT @", row."FullText")
            rt_dict[row."ScreenName"] += row."Retweet-Count"
        end
    end


    #4. and now add edges
    for row in eachrow(df_en[1:nodenumber,:])
        if (!(haskey(id_dict, row."To-User-Id")) || !(haskey(id_dict, row."From-User-Id")))
            continue
        end
        if row."To-User-Id" != -1
            #add a entry if not already present
            if !(haskey(name_dict,id_dict[row."From-User-Id"]))
                if !(haskey(name_dict,id_dict[row."To-User-Id"]))
                    add_vertex!(meta_graph)
                    #add entry to indexarr
                    push!(indexarr,length(indexarr)+1)
                    #position of name to dict
                    name = id_dict[row."To-User-Id"]
                    name_dict[name] = indexarr[end]
                    #and name to nodelabels
                    push!(nodelabels,id_dict[row."To-User-Id"])
                    #and retweet count
                    if !occursin("RT @", row."FullText")
                        rt_dict[row."ScreenName"] = row."Retweet-Count"
                        name_dict[row."ScreenName"] = indexarr[end]
                    end
                end
                add_vertex!(meta_graph)
                #add entry to indexarr
                push!(indexarr,length(indexarr)+1)
                #position of name to dict
                name_dict[id_dict[row."From-User-Id"]] = indexarr[end]
                #and name to nodelabels
                push!(nodelabels,id_dict[row."From-User-Id"])
                #and retweet count
                if !occursin("RT @", row."FullText")
                    rt_dict[row."ScreenName"] = row."Retweet-Count"
                    name_dict[row."ScreenName"] = indexarr[end]
                end
                add_edge!(meta_graph,name_dict[id_dict[row."From-User-Id"]],name_dict[id_dict[row."To-User-Id"]])
                continue
            end
            if !(haskey(name_dict,id_dict[row."To-User-Id"]))
                if !(haskey(name_dict,id_dict[row."From-User-Id"]))
                    add_vertex!(meta_graph)
                    #add entry to indexarr
                    push!(indexarr,length(indexarr)+1)
                    #position of name to dict
                    name_dict[id_dict[row."From-User-Id"]] = indexarr[end]
                    #and name to nodelabels
                    push!(nodelabels,id_dict[row."From-User-Id"])
                    #and retweet count
                    if !occursin("RT @", row."FullText")
                        rt_dict[row."ScreenName"] = row."Retweet-Count"
                        name_dict[row."ScreenName"] = indexarr[end]
                    end
                end
                add_vertex!(meta_graph)
                #add entry to indexarr
                push!(indexarr,length(indexarr)+1)
                #position of name to dict
                name_dict[id_dict[row."To-User-Id"]] = indexarr[end]
                #and name to nodelabels
                push!(nodelabels,id_dict[row."To-User-Id"])
                #and retweet count
                if !occursin("RT @", row."FullText")
                    rt_dict[row."ScreenName"] = row."Retweet-Count"
                end
                add_edge!(meta_graph,name_dict[id_dict[row."From-User-Id"]],name_dict[id_dict[row."To-User-Id"]])
                continue
            end
            add_edge!(meta_graph,name_dict[id_dict[row."From-User-Id"]],name_dict[id_dict[row."To-User-Id"]])
        end
    end
    #and get the rt counts in order
    nodesizes = ones(length(nodelabels))
    nodecolors = [colorant"yellow" for i in 1:length(nodelabels)]
    @simd for i in collect(keys(rt_dict))
        #set the index of the nodesizes array to the screen name -> position mapping of the name dict
        #to the rt count entry of the rt dict
        try
            nodesizes[name_dict[i]] = rt_dict[i]
        catch
        end
        try
            #println("bot dict $(bot_dict[i])")
            #println("name dict $(name_dict[i])")
            #b = Int16(round(scale(0,1,0,256,bot_dict[i])))
            #b > 256 && (b = 256)
            nodecolors[name_dict[i]]=cgrad(:thermal)[bot_dict[i]]
        catch
        end
    end
    nodesizes = sqrt.(sqrt.(nodesizes))
    return meta_graph, nodelabels, nodesizes, nodecolors
end

df_random = df_en[shuffle(axes(df_en,1)),:]
@time graph,labels,sizes,colors = create_graph(df_en,nrow(df_en))
@time graph_h,labels_h,sizes_h,colors_h = create_graph(df_hashtag, nrow(df_hashtag))
plot_graph(graph,labels)


#
# graph, labels = create_graph(c[end-2005:end,:])
# plot_graph(graph,labels)

#for each x days in timeframe
function for_x_days(x,df_en,func)
    first_day = df_en[1,:Created]
    current_day = df_en[1,:Created]
    nrows = 1
    #container
    arr = []
    #a sweet exit condition
    while nrows>0
        #select all days between current_day and current_day+x days
        temp_df = @where(df_en, :Created.>=current_day,
                                :Created.<current_day+Dates.Day(x))
        #check if there are entries and break if there are none
        nrows = nrow(temp_df)
        println(nrows)
        nrows == 0 && continue
        #call the callback
        result = func(temp_df)
        #and add the output to the array which is returned
        push!(arr,result)
        #increment the counter
        current_day = current_day+Dates.Day(x)
    end
    return arr
end

function tf(df)
    println(nrow(df))
end
# for_x_days(7,df_full,tf)
