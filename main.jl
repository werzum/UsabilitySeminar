using CSV, DataFrames, LightGraphs, MetaGraphs, GraphPlot, Plots, StatsPlots, StatsBase, Plots.PlotMeasures, Cairo, Compose
using PackageCompiler, Dictionaries, Distributed, Dates, JSON3, JSON, Random, StringEncodings, DataFramesMeta, DataStructures

include("plotting.jl")
include("data_processing.jl")

df_en = DataFrame!(CSV.File("botometer.csv";threaded=false))
