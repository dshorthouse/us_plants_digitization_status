#!/usr/bin/env ruby
# encoding: utf-8

require 'rest_client'
require 'csv'
require 'json'
require 'colorize'

url = 'https://www.gbif.org/api/dataset/search?publishing_country=US&type=OCCURRENCE&limit=1000'
response = RestClient.get(url)
data = JSON.parse(response, { symbolize_names: true })
datasets = data[:results].map{|d| d.slice(:key, :title, :_key)}

CSV.open("gbif_us_datasets.csv", "w") do |csv|
  csv << ["datasetKey", "name", "created", "modified"]
  datasets.each do |dataset|
    puts dataset[:key].green
    csv << [
      dataset[:key],
      dataset[:title],
      dataset[:_key][:created],
      dataset[:_key][:modified]
    ]
  end
end
