def dumpFlightsToImage_2(self, listFlights):
        import gmplot 
        tracks = []
        
        img = Image.new('RGBA', (MAP_WIDTH, MAP_HEIGHT), (255, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        
        lons = np.vectorize(lambda f: f.lon)
        lats = np.vectorize(lambda f: f.lat)
        
        gmap3 = gmplot.GoogleMapPlotter(45.216150, 5.842767, 8) 

        for flight in listFlights:
            longitudes = lons(flight.fixes)
            latitudes = lats(flight.fixes)
            flightPoints = list(zip(latitudes.tolist(), longitudes.tolist()))
            
            
            # Plot method Draw a line in 
            # between given coordinates 
            #gmap3.heatmap(latitudes.tolist(), longitudes.tolist() ) 
            gmap3.plot(latitudes.tolist(), longitudes.tolist(),'cornflowerblue', edge_width = 1) 
            
        gmap3.draw( "/Users/llauner/Downloads/map13.html" )  
        
        
def dumpFlightsToImage_3(self, listFlights):
	from staticMap import StaticMap, Line
	tracks = []
	
	m = StaticMap(800, 800, 80)
	
	lons = np.vectorize(lambda f: f.lon)
	lats = np.vectorize(lambda f: f.lat)
	

	for flight in listFlights:
		longitudes = lons(flight.fixes)
		latitudes = lats(flight.fixes)
		flightPoints = list(zip(latitudes.tolist(), longitudes.tolist()))
	
		coordinates = np.stack((longitudes.tolist(), latitudes.tolist()), axis=1)
		
		line = Line(coordinates, '#D2322D', 1)
		m.add_line(line)

	image = m.render(zoom=6)
	image.save('/Users/llauner/Downloads/z_tracks.png')