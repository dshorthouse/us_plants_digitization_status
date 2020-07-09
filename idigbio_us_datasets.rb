#!/usr/bin/env ruby
# encoding: utf-8

require 'rest_client'
require 'csv'
require 'json'
require 'colorize'

url = 'https://search.idigbio.org/v2/search/recordsets/?limit=5000'
response = RestClient.get(url)
data = JSON.parse(response, { symbolize_names: true })
datasets = data[:items].map{|d| d.slice(:uuid, :data)}

CSV.open("idigbio_us_datasets.csv", "w") do |csv|
  csv << ["uuid", "name"]
  datasets.each do |dataset|
    next if dataset[:data].empty?
    puts dataset[:data][:collection_name].green
    csv << [
      dataset[:uuid],
      dataset[:data][:collection_name]
    ]
  end
end
