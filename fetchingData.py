import os


def write_into_file_from_gz(url,type_file,date):
	f = open(type_file+"_"+date+".csv","w")
	os.system("curl -O "+url);
	gz_filename = url.split('/')[-1]
	os.system("find . -name '"+gz_filename+"' -exec gzip -d {} \;")
	g = open(gz_filename[:-3],"r")
	for line in g:
		f.write(line)
	f.close()
	g.close()
	os.system("rm "+gz_filename[:-3])

def write_into_file_csv(url,type_file,date):
	f = open(type_file+"_"+date+".csv","w")
	os.system("curl -O "+url);
	csv_filename = url.split('/')[-1]

	g = open(csv_filename,"r")
	for line in g:
		f.write(line)
	f.close()
	g.close()
	os.system("rm "+csv_filename);

def write_into_file_geojson(url,type_file,date):
	f = open(type_file+"_"+date+".geojson","w")
	os.system("curl -O "+url);
	csv_filename = url.split('/')[-1]

	g = open(csv_filename,"r")
	for line in g:
		f.write(line)
	f.close()
	g.close()
	os.system("rm "+csv_filename);

def main():
	dates = ["2017-10-02","2017-09-02","2017-08-02","2017-07-02","2017-06-02","2017-05-02","2017-04-02","2017-03-02","2017-02-02","2017-01-01","2016-12-03","2016-11-02"]
	for date in dates:
		src_detailed_listings = "http://data.insideairbnb.com/united-states/ny/new-york-city/"+date+"/data/listings.csv.gz"
		src_detailed_calendar = "http://data.insideairbnb.com/united-states/ny/new-york-city/"+date+"/data/calendar.csv.gz"
		src_detailed_reviews = "http://data.insideairbnb.com/united-states/ny/new-york-city/"+date+"/data/reviews.csv.gz"
		src_summarized_listings = "http://data.insideairbnb.com/united-states/ny/new-york-city/"+date+"/visualisations/listings.csv"
		src_summarized_reviews = "http://data.insideairbnb.com/united-states/ny/new-york-city/"+date+"/visualisations/reviews.csv"
		src_neighbourhood_list = "http://data.insideairbnb.com/united-states/ny/new-york-city/"+date+"/visualisations/neighbourhoods.csv"
		src_geojson_neighbourhood = "http://data.insideairbnb.com/united-states/ny/new-york-city/"+date+"/visualisations/neighbourhoods.geojson"
		
		write_into_file_from_gz(src_detailed_listings,"detailed_listings",date)
		write_into_file_from_gz(src_detailed_calendar,"detailed_calendar",date)
		write_into_file_from_gz(src_detailed_reviews,"detailed_reviews",date)	

		write_into_file_csv(src_summarized_listings,"summarized_listings",date)
		write_into_file_csv(src_summarized_reviews,"summarized_reviews",date)
		write_into_file_csv(src_neighbourhood_list,"neighbourhood_list",date)
		write_into_file_geojson(src_geojson_neighbourhood,"neighbourhoods_geojson",date)
		break


main()		