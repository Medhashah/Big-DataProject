# -*- coding: UTF-8 -*-
import json
import os
import re

from pyspark import SparkContext

label_list = ['person_name', 'business_name', 'phone_number', 'address', 'street_name', 'city', 'neighborhood',
              'lat_lon_cord', 'zip_code', 'borough', 'school_name', 'color', 'car_make', 'city_agency', 'area_of_study',
              'subject_in_school', 'school_level', 'college_name', 'website', 'building_classification', 'vehicle_type',
              'location_type', 'park_playground', 'other']

cities = """accord,adams village,adams center,addison village,afton village,airmont village,akron village,albany,albertson,albion village,alden village,alexander village,alexandria bay village,alfred village,allegany village,almond village,altamont village,altmar village,altona,amagansett,amenia,ames village,amityville village,amsterdam,andes village,andover village,angelica village,angola village,angola on the lake,antwerp village,apalachin,aquebogue,arcade village,ardsley village,argyle village,arkport village,arlington,armonk,asharoken village,athens village,atlantic beach village,attica village,auburn,aurora village,au sable forks,averill park,avoca village,avon village,babylon village,bainbridge village,baiting hollow,baldwin,baldwin harbor,baldwinsville village,ballston spa village,balmville,bardonia,barker village,barneveld village,barnum island,batavia,bath village,baxter estates village,bay park,bayport,bay shore,bayville village,baywood,beacon,beaverdam lake-salisbury mills,bedford,bellerose village,bellerose terrace,belle terre village,bellmore,bellport village,belmont village,bemus point village,bergen village,bethpage,big flats,big flats airport,billington heights,binghamton,black river village,blasdell village,blauvelt,bloomfield village,bloomingburg village,blue point,bohemia,bolivar village,boonville village,brasher falls-winthrop,brentwood,brewerton,brewster village,brewster hill,briarcliff manor village,bridgehampton,bridgeport,bridgewater village,brighton,brightwaters village,brinckerhoff,broadalbin village,brockport village,brocton village,bronxville village,brookhaven,brookville village,brownville village,brushton village,buchanan village,buffalo,burdett village,burke village,cairo,calcium,caledonia village,callicoon,calverton,cambridge village,camden village,camillus village,canajoharie village,canandaigua,canaseraga village,canastota village,candor village,canisteo village,canton village,cape vincent village,carle place,carmel hamlet,carthage village,cassadaga village,castile village,castleton-on-hudson village,castorland village,cato village,catskill village,cattaraugus village,cayuga village,cayuga heights village,cazenovia village,cedarhurst village,celoron village,centereach,center moriches,centerport,central islip,central square village,central valley,centre island village,champlain village,chappaqua,chateaugay village,chatham village,chaumont village,cheektowaga,cherry creek village,cherry valley village,chester village,chestnut ridge village,chittenango village,churchville village,clarence center,clark mills,claverack-red mills,clayton village,clayville village,cleveland village,clifton springs village,clinton village,clintondale,clyde village,cobleskill village,coeymans,cohocton village,cohoes,cold brook village,cold spring village,cold spring harbor,colonie village,commack,congers,constableville village,constantia,cooperstown village,copake lake,copenhagen village,copiague,coram,corfu village,corinth village,corning,cornwall on hudson village,cortland,cortland west,country knolls,cove neck village,coxsackie village,cragsmoor,croghan village,crompond,croton-on-hudson village,crown heights,crugers,cuba village,cumberland head,cutchogue,dannemora village,dansville village,deer park,deferiet village,delanson village,delevan village,delhi village,delmar,depauville,depew village,deposit village,dering harbor village,deruyter village,dexter village,dix hills,dobbs ferry village,dolgeville village,dover plains,dresden village,dryden village,duane lake,duanesburg,dundee village,dunkirk,earlville village,east atlantic beach,east aurora village,eastchester,east farmingdale,east garden city,east glenville,east greenbush,east hampton village,east hampton north,east hills village,east islip,east ithaca,east kingston,east marion,east massapequa,east meadow,east moriches,east nassau village,east northport,east norwich,east patchogue,eastport,east quogue,east randolph village,east rochester village,east rockaway village,east shoreham,east syracuse village,east williston village,eatons neck,eden,edwards village,elba village,elbridge village,ellenville village,ellicottville village,ellisburg village,elma center,elmira,elmira heights village,elmont,elmsford village,elwood,endicott village,endwell,esperance village,evans mills village,fabius village,fair haven village,fairmount,fairport village,fairview cdp (dutchess county),fairview cdp (westchester county),falconer village,farmingdale village,farmingville,farnham village,fayetteville village,fire island,firthcliffe,fishers island,fishkill village,flanders,fleischmanns village,floral park village,florida village,flower hill village,fonda village,forest home,forestville village,fort ann village,fort drum,fort edward village,fort johnson village,fort montgomery,fort plain village,fort salonga,frankfort village,franklin village,franklin square,franklinville village,fredonia village,freeport village,freeville village,frewsburg,friendship,fulton,fultonville village,gainesville village,galeville,galway village,gang mills,garden city village,garden city park,garden city south,gardiner,gardnertown,gasport,gates-north gates,geneseo village,geneva,germantown,ghent,gilbertsville village,gilgo-oak beach-captree,glasco,glen cove,glen head,glen park village,glens falls,glens falls north,glenwood landing,gloversville,golden's bridge,gordon heights,goshen village,gouverneur village,gowanda village,grand view-on-hudson village,granville village,great bend,great neck village,great neck estates village,great neck gardens,great neck plaza village,great river,greece,greene village,green island village,greenlawn,greenport village,greenport west,greenvale,greenville cdp (greene county),greenville cdp (westchester county),greenwich village,greenwood lake village,groton village,hagaman village,halesite,hamburg village,hamilton village,hammond village,hammondsport village,hampton bays,hampton manor,hancock village,hannibal village,harbor hills,harbor isle,harriman village,harris hill,harrison village,harrisville village,hartsdale,hastings-on-hudson village,hauppauge,haverstraw village,haviland,hawthorne,head of the harbor village,hempstead village,heritage hills,herkimer village,hermon village,herricks,herrings village,heuvelton village,hewlett,hewlett bay park village,hewlett harbor village,hewlett neck village,hicksville,high falls,highland,highland falls village,highland mills,hillburn village,hillcrest,hillside,hillside lake,hilton village,hobart village,holbrook,holland,holland patent village,holley village,holtsville,homer village,honeoye falls village,hoosick falls village,hopewell junction,hornell,horseheads village,horseheads north,houghton,hudson,hudson falls village,hunter village,huntington,huntington bay village,huntington station,hurley,ilion village,interlaken village,inwood,irondequoit,irvington village,islandia village,island park village,islip,islip terrace,ithaca,jamesport,jamestown,jamestown west,jefferson heights,jefferson valley-yorktown,jeffersonville village,jericho,johnson city village,johnstown,jordan village,kaser village,keeseville village,kenmore village,kensington village,kerhonkson,kinderhook village,kings park,kings point village,kingston,kiryas joel village,lackawanna,lacona village,la fargeville,lake carmel,lake erie beach,lake george village,lake grove village,lake katrine,lakeland,lake luzerne-hadley,lake mohegan,lake placid village,lake ronkonkoma,lake success village,lakeview,lakewood village,lancaster village,lansing village,larchmont village,lattingtown village,laurel,laurel hollow village,laurens village,lawrence village,leeds,leicester village,le roy village,levittown,lewiston village,liberty village,lido beach,lima village,lime lake-machias,limestone village,lincolndale,lincoln park,lindenhurst village,lisle village,little falls,little valley village,liverpool village,livingston manor,livonia village,lloyd harbor village,lockport,locust valley,lodi village,long beach,lorenz park,lowville village,lynbrook village,lyncourt,lyndonville village,lyon mountain,lyons village,lyons falls village,macedon village,mcgraw village,madison village,mahopac,malden,malone village,malverne village,malverne park oaks,mamaroneck village,manchester village,manhasset,manhasset hills,manlius village,mannsville village,manorhaven village,manorville,marathon village,marcellus village,margaretville village,mariaville lake,marlboro,massapequa,massapequa park village,massena village,mastic,mastic beach,matinecock village,mattituck,mattydale,maybrook village,mayfield village,mayville village,mechanicstown,mechanicville,medford,medina village,medusa,melrose park,melville,menands village,meridian village,merrick,mexico village,middleburgh village,middle island,middleport village,middletown,middleville village,milford village,millbrook village,miller place,millerton village,mill neck village,millport village,milton cdp (saratoga county),milton cdp (ulster county),mineola village,minetto,mineville-witherbee,minoa village,mohawk village,monroe village,monsey,montauk,montebello village,montgomery village,monticello village,montour falls village,mooers,moravia village,moriches,morris village,morrisonville,morristown village,morrisville village,mount ivy,mount kisco village,mount morris village,mount sinai,mount vernon,munnsville village,munsey park village,munsons corners,muttontown village,myers corner,nanuet,napanoch,napeague,naples village,narrowsburg,nassau village,natural bridge,nedrow,nelliston village,nelsonville village,nesconset,newark village,newark valley village,new berlin village,newburgh,new cassel,new city,newfane,newfield hamlet,new hartford village,new hempstead village,new hyde park village,new paltz village,newport village,new rochelle,new square village,new suffolk,new windsor,new york,new york mills village,niagara falls,nichols village,niskayuna,nissequogue village,niverville,norfolk,north amityville,northampton,north babylon,north ballston spa,north bay shore,north bellmore,north bellport,north boston,north collins village,northeast ithaca,north great river,north haven village,north hills village,north hornell village,north lindenhurst,north lynbrook,north massapequa,north merrick,north new hyde park,north patchogue,northport village,north sea,north syracuse village,north tonawanda,north valley stream,northville village,northville,north wantagh,northwest harbor,northwest ithaca,norwich,norwood village,noyack,nunda village,nyack village,oakdale,oakfield village,ocean beach village,oceanside,odessa village,ogdensburg,olcott,old bethpage,old brookville village,old field village,old westbury village,olean,oneida,oneida castle village,oneonta,orangeburg,orange lake,orchard park village,orient,oriskany village,oriskany falls village,ossining village,oswego,otego village,otisville village,ovid village,owego village,oxford village,oyster bay,oyster bay cove village,painted post village,palatine bridge village,palenville,palmyra village,panama village,parc,parish village,patchogue village,pattersonville-rotterdam junction,pawling village,peach lake,pearl river,peconic,peekskill,pelham village,pelham manor village,penn yan village,perry village,perrysburg village,peru,phelps village,philadelphia village,philmont village,phoenicia,phoenix village,piermont village,pike village,pine bush,pine hill,pine plains,pittsford village,plainedge,plainview,plandome village,plandome heights village,plandome manor village,plattekill,plattsburgh,plattsburgh west,pleasant valley,pleasantville village,poestenkill,point lookout,poland village,pomona village,poquott village,port byron village,port chester village,port dickinson village,port ewen,port henry village,port jefferson village,port jefferson station,port jervis,port leyden village,portville village,port washington,port washington north village,potsdam village,poughkeepsie,preston-potter hollow,prospect village,pulaski village,putnam lake,quioque,quogue village,randolph village,ransomville,rapids,ravena village,red creek village,redford,red hook village,red oaks mill,redwood,remsen village,remsenburg-speonk,rensselaer,rensselaer falls village,rhinebeck village,richburg village,richfield springs village,richmondville village,richville village,ridge,rifton,ripley,riverhead,riverside village,riverside,rochester,rock hill,rockville centre village,rocky point,rome,ronkonkoma,roosevelt,roscoe,rosendale village,roslyn village,roslyn estates village,roslyn harbor village,roslyn heights,rotterdam,round lake village,rouses point village,rushville village,russell gardens village,rye,rye brook village,sackets harbor village,saddle rock village,saddle rock estates,sagaponack,sag harbor village,st. bonaventure,st. james,st. johnsville village,salamanca,salem village,salisbury,saltaire village,sand ridge,sands point village,sandy creek village,saranac lake village,saratoga springs,saugerties village,saugerties south,savona village,sayville,scarsdale village,schaghticoke village,schenectady,schoharie village,schuylerville village,scotchtown,scotia village,scotts corners,scottsville village,sea cliff village,seaford,searingtown,selden,seneca falls village,seneca knolls,setauket-east setauket,sharon springs village,shelter island,shelter island heights,shenorock,sherburne village,sherman village,sherrill,shinnecock hills,shirley,shokan,shoreham village,shortsville village,shrub oak,sidney village,silver creek village,silver springs village,sinclairville village,skaneateles village,sleepy hollow village,sloan village,sloatsburg village,smallwood,smithtown,smyrna village,sodus village,sodus point village,solvay village,sound beach,southampton village,south corning village,south dayton village,south fallsburg,south farmingdale,south floral park village,south glens falls village,south hempstead,south hill,south huntington,south lockport,south nyack village,southold,southport,south valley stream,spackenkill,speculator village,spencer village,spencerport village,springs,spring valley village,springville village,staatsburg,stamford village,stannards,star lake,stewart manor village,stillwater village,stone ridge,stony brook,stony point,stottville,suffern village,sylvan beach village,syosset,syracuse,tannersville village,tappan,tarrytown village,terryville,theresa village,thiells,thomaston village,thornwood,tillson,tivoli village,tonawanda,tonawanda,town line,tribes hill,troy,trumansburg village,tuckahoe,tuckahoe village,tully village,tupper lake village,turin village,tuxedo park village,unadilla village,uniondale,union springs village,unionville village,university gardens,upper brookville village,upper nyack village,utica,vails gate,valatie village,valhalla,valley cottage,valley falls village,valley stream village,van etten village,vernon village,verplanck,victor village,victory village,village green,village of the branch village,viola,voorheesville village,waddington village,wading river,wainscott,walden village,walker valley,wallkill,walton village,walton park,wampsville village,wantagh,wappingers falls village,warrensburg,warsaw village,warwick village,washington heights,washingtonville village,waterford village,waterloo village,watermill,watertown,waterville village,watervliet,watkins glen village,waverly village,wayland village,webster village,weedsport village,wellsburg village,wellsville village,wesley hills village,west babylon,west bay shore,westbury village,west carthage village,west elmira,west end,westfield village,west glens falls,westhampton,westhampton beach village,west hampton dunes village,west haverstraw village,west hempstead,west hills,west hurley,west islip,westmere,west nyack,weston mills,west point,west sand lake,west sayville,west seneca,westvale,west winfield village,wheatley heights,whitehall village,white plains,whitesboro village,whitney point village,williamsville village,williston park village,wilson village,windham,windsor village,wolcott village,woodbury,woodmere,woodridge village,woodsburgh village,woodstock,wurtsboro village,wyandanch,wynantskill,wyoming village,yaphank,yonkers,yorkshire,yorktown heights,yorkville village,youngstown village,zena"""
counties = """albany,allegany,bronx,broome,cattaraugus,cayuga,chautauqua,chemung,chenango,clinton,columbia,cortland,delaware,dutchess,erie,essex,franklin,fulton,genesee,greene,hamilton,herkimer,jefferson,kings,lewis,livingston,madison,monroe,montgomery,nassau,new york city,niagara,oneida,onondaga,ontario,orange,orleans,oswego,otsego,putnam,queens,rensselaer,richmond,rockland,st. lawrence,saratoga,schenectady,schoharie,schuyler,seneca,steuben,suffolk,sullivan,tioga,tompkins,ulster,warren,washington,wayne,westchester,wyoming,yates"""
borough = "bronx,brooklyn,manhattan,queens,staten island,the bronx,bkly"
borough_ab = "K,M,Q,R,X"
school_levels = "elementary,elementary school,k-8,k-8 school,middle,middle school,high school,high school transfer,k-3,k-3 school,yabc"
park_types = "playground,park,field,garden,senior center,gardens,parkway"
colors = "amber,ash,asphalt,auburn,avocado,aquamarine,azure,beige,bisque,black,blue,bone,bordeaux,brass,bronze,brown,burgundy,camel,caramel,canary,celeste,cerulean,champagne,charcoal,chartreuse,chestnut,chocolate,citron,claret,coal,cobalt,coffee,coral,corn,cream,crimson,cyan,denim,desert,ebony,ecru,emerald,feldspar,fuchsia,gold,gray,green,heather,indigo,ivory,jet,khaki,lime,magenta,maroon,mint,navy,olive,orange,pink,plum,purple,red,rust,salmon,sienna,silver,snow,steel,tan,teal,tomato,violet,white,yellow"
business_pattern = "corp,inc,llc,bar,service,cafe,market,mall,restaurant,taste,pizza,fusion,food"
school_name_pattern = "acad,academic,school,p.s.,i.s.,hs,sped,m.s.,ps/is,j.h.s."
neighborhoods = "bath,beach,allerton,battery,arverne,annadale,bay,ridge,bathgate,beekman,astoria,arden,heights,bedford,stuyvesant,baychester,carnegie,astoria,heights,arlington,bensonhurst,bedford,chelsea,auburndale,arrochar,bergen,belmont,chinatown,terrace,terrace,boerum,bronxdale,civic,center,bayside,bloomfield,clinton,bayswater,bulls,head,brighton,beach,harlem,beechhurst,butler,manor,broadway,junction,castle,hill,east,village,bellaire,castleton,corners,brooklyn,heights,city,island,financial,district,belle,harbor,charleston,brownsville,claremont,village,flatiron,bellerose,chelsea,bushwick,clason,point,gramercy,blissville,clifton,canarsie,concourse,greenwich,village,breezy,point,concord,carroll,gardens,concourse,village,hamilton,heights,briarwood,dongan,hills,city,line,co-op,city,harlem,(central),broad,channel,egbertville,clinton,hill,country,club,herald,square,brookville,elm,park,cobble,hill,east,tremont,hudson,square,cambria,heights,eltingville,coney,island,eastchester,inwood,clearview,emerson,hill,crown,heights,edenwald,lenox,hill,college,point,fox,hills,cypress,hills,edgewater,park,lincoln,square,douglaston,graniteville,ditmas,park,fieldston,little,italy,dutch,kills,grant,city,downtown,fordham,lower,east,side,east,elmhurst,grasmere,dumbo,high,bridge,manhattan,valley,edgemere,great,kills,dyker,heights,hunts,point,manhattanville,elmhurst,greenridge,east,flatbush,kingsbridge,midtown,south,far,rockaway,grymes,hill,east,new,york,kingsbridge,heights,midtown,floral,park,heartland,village,east,williamsburg,longwood,morningside,heights,flushing,howland,hook,farragut,marble,hill,murray,hill,flushing,(downtown),huguenot,flatbush,melrose,noho,forest,hills,lighthouse,hill,flatlands,morris,heights,roosevelt,island,forest,hills,gardens,livingston,fort,greene,morris,park,soho,fresh,meadows,manor,heights,fort,hamilton,morrisania,south,village,glen,oaks,mariner's,harbor,fulton,ferry,mott,haven,stuyvesant,town,glendale,midland,beach,georgetown,mount,eden,sutton,place,hammels,new,brighton,gerritsen,beach,mount,hope,times,square,hillcrest,new,dorp,gowanus,north,riverdale,tribeca,hollis,new,dorp,beach,gravesend,norwood,tudor,city,holliswood,new,springville,greenpoint,olinville,turtle,bay,howard,beach,oakwood,highland,park,parkchester,union,square,hunters,point,old,place,homecrest,pelham,bay,upper,east,side,jackson,heights,old,town,kensington,pelham,gardens,upper,west,side,jamaica,park,hill,kings,highway,pelham,parkway,wall,street,jamaica,center,pleasant,plains,manhattan,beach,port,morris,washington,heights,jamaica,estates,port,ivory,manhattan,terrace,riverdale,west,village,jamaica,hills,port,richmond,mapleton,schuylerville,yorkville,kew,gardens,prince's,bay,marine,park,soundview,kew,gardens,hills,randall,manor,midwood,spuyten,duyvil,laurelton,richmond,town,mill,basin,throgs,neck,lefrak,city,richmond,valley,mill,island,unionport,lindenwood,rosebank,navy,yard,university,heights,little,neck,rossville,new,lots,van,nest,long,island,city,sandy,ground,north,side,wakefield,malba,shore,acres,ocean,hill,west,farms,maspeth,silver,lake,ocean,parkway,westchester,square,middle,village,south,beach,paerdegat,basin,williamsbridge,murray,hill,st.,george,park,slope,woodlawn,neponsit,stapleton,plum,beach,new,hyde,park,sunnyside,prospect,heights,north,corona,todt,hill,prospect,lefferts,gardens,oakland,gardens,tompkinsville,prospect,park,south,ozone,park,tottenville,red,hook,pomonok,travis,remsen,village,queens,village,ward,hill,rugby,queensboro,hill,west,brighton,sea,gate,ravenswood,westerleigh,sheepshead,bay,rego,park,willowbrook,south,side,richmond,hill,woodrow,spring,creek,ridgewood,starrett,city,rochdale,stuyvesant,heights,rockaway,park,sunset,park,rosedale,tompkins,park,north,roxbury,vinegar,hill,seaside,weeksville,somerville,west,brighton,south,corona,williamsburg,south,jamaica,windsor,terrace,south,ozone,park,wingate,springfield,gardens,st.,albans,steinway,sunnyside,sunnyside,gardens,utopia,whitestone,woodhaven,woodside"
street_pattern = "street,st,road,rd,ave,7avenue,blvd,drive,court,place,pl,boulevard,way,parkway,av"
color_ab = "BK,BL,BR,GL,GY,MR,OR,PK,PR,RD,TN,WH,YW,LT,DK,NOCL"
car_types = "FIRE,CONV,SEDN,SUBN,4DSD,2DSD,H/WH,ATV,MCY,H/IN,TRAV,MOBL,TR/E,T/CR,TR/C,SWT,W/DR,W/SR,FPM,MCC,EMVR,TRAC,N/A,DELV,DUMP,FLAT,PICK,STAK,TANK,REFG,LOCO,CUST,RPLC,AMBU,P/SH,RBM,R/RD,RD/S,S/SP,SN/P,TOW,VAN,UTIL,POLE,BOAT,H/TR,SEMI,TRLR,LTRL,LSVT,BUS,LIM,HRSE,TAXI,DCOM,CMIX,MOPD,MFH,SNOW,LSV".lower()
building_classification_pattern = "WALK-UP,ELEVATOR,WALK-UP,CONDOPS".lower()
subjects = "ENGLISH,MATH,SCIENCE,SOCIAL STUDIES,ALGEBRA,CHEMISTRY,EARTH SCIENCE,ECONOMICS,GEOMETRY,HISTORY,ENVIRONMENT,PHYSICS,US GOVERNMENT".lower()
study_areas = "ANIMAL SCIENCE,ARCHITECTURE,BUSINESS.COMMUNICATIONS,COMPUTER SCIENCE & TECHNOLOGY,COSMETOLOGY,CULINARY ARTS,ENGINEERING,ENVIRONMENTAL SCIENCE,ENVIRONMENTAL SCIENCE,FILM/VIDEO,HEALTH PROFESSIONS,HOSPITALITY, TRAVEL AND TOURISM,HUMANITIES & INTERDISCIPLINARY,JROTC,LAW & GOVERNMENT,PERFORMING ARTS,PERFORMING ARTS/VISUAL ART & DESIGN,VISUAL ART & DESIGN,SCIENCE & MATH,TEACHING,ZONED".lower()
location_types = "airport,amusement_park,aquarium,art_gallery,atm,bakery,bank,bar,beauty_salon,bicycle_store,book_store,bowling_alley,bus_station,cafe,campground,car_dealer,car_rental,car_repair,car_wash,casino,cemetery,church,city_hall,clothing_store,convenience_store,courthouse,dentist,department_store,doctor,drugstore,electrician,electronics_store,embassy,fire_station,florist,funeral_home,furniture_store,gas_station,grocery_or_supermarket,gym,hair_care,hardware_store,hindu_temple,home_goods_store,hospital,insurance_agency,jewelry_store,laundry,lawyer,library,light_rail_station,liquor_store,local_government_office,locksmith,lodging,meal_delivery,meal_takeaway,mosque,movie_rental,movie_theater,moving_company,museum,night_club,painter,park,parking,pet_store,pharmacy,physiotherapist,plumber,police,post_office,primary_school,real_estate_agency,restaurant,roofing_contractor,rv_park,school,secondary_school,shoe_store,shopping_mall,spa,stadium,storage,store,subway_station,supermarket,synagogue,taxi_stand,tourist_attraction,train_station,transit_station,travel_agency,university,veterinary_care,zoo, ABANDONED,BUILDING,AIRPORT,TERMINAL,ATM,BANK,BAR/NIGHT,CLUB,BEAUTY,&,NAIL,SALON,BOOK/CARD,BRIDGE,BUS,(NYC,TRANSIT),BUS,(OTHER),BUS,STOP,BUS,TERMINAL,CANDY,STORE,CEMETERY,CHAIN,STORE,CHECK,CASHING,BUSINESS,CHURCH,CLOTHING/BOUTIQUE,COMMERCIAL,BUILDING,CONSTRUCTION,SITE,DEPARTMENT,STORE,DOCTOR/DENTIST,OFFICE,DRUG,STORE,DRY,CLEANER/LAUNDRY,FACTORY/WAREHOUSE,FAST,FOOD,FERRY/FERRY,TERMINAL,FOOD,SUPERMARKET,GAS,STATION,GROCERY/BODEGA,GYM/FITNESS,FACILITY,HIGHWAY/PARKWAY,HOSPITAL,HOTEL/MOTEL,JEWELRY,LIQUOR,STORE,LOAN,COMPANY,MAILBOX,INSIDE,MAILBOX,OUTSIDE,MARINA/PIER,MOSQUE,OPEN,AREAS,(OPEN,LOTS),OTHER,OTHER,HOUSE,OF,WORSHIP,PARK/PLAYGROUND,PARKING,LOT/GARAGE,(PRIVATE),PARKING,LOT/GARAGE,(PUBLIC),PHOTO/COPY,PRIVATE/PAROCHIAL,SCHOOL,PUBLIC,BUILDING,PUBLIC,SCHOOL,RESIDENCE,APT.,HOUSE,RESIDENCE,PUBLIC,HOUSING,RESIDENCE-HOUSE,RESTAURANT/DINER,SHOE,SMALL,MERCHANT,SOCIAL,CLUB/POLICY,STORAGE,FACILITY,STORE,UNCLASSIFIED,STREET,SYNAGOGUE,TAXI,(LIVERY,LICENSED),TAXI,(YELLOW,LICENSED),TAXI/LIVERY,(UNLICENSED),TELECOMM.,STORE,TRAMWAY,TRANSIT,NYC,SUBWAY,TRANSIT,FACILITY,(OTHER),TUNNEL,VARIETY,STORE,VIDEO,STORE".lower()
agencies = "ACS,BIC,CCHR,CCRB,DCA,DCAS,DCLA,DCP,DDC,DEP,DFTA,DHS,DOB,DOC,DOE,DOF,DOHMH,DOI,DOITT,DOP,DORIS,DOT,DPR,DSNY,DSS,DYCD,EDC,FDNY,HPD,HRA,LAW,LPC,MOCJ,MOCS,NYCEM,NYPD,OATH,SBS,TLC"
car_make = "ABARTH,ALFA,ROMEO,ASTON,MARTIN,AUSTIN,BEDFORD,BENTLEY,BOLWELL,BUFORI,CADILLAC,CATERHAM,CHERY,CHEVROLET,CHRYSLER,CITROEN,DAEWOO,DAIHATSU,DODGE,FIAT,GEELY,GREAT,WALL,HINO,HUMMER,INFINITI,ISUZU,JAGUAR,JEEP,LAND,ROVER,LEXUS,LOTUS,MINI,MITSUBISHI,OPEL,PORSCHE,PROTON,RANGE,ROVER,RENAULT,SAAB,SKODA,SSANGYONG,SUBARU,SUZUKI,TATA,VOLVO"
# list of 1000 most popular surnames in USA
Last_names = "SMITH,JOHNSON,WILLIAMS,JONES,BROWN,DAVIS,MILLER,WILSON,MOORE,TAYLOR,ANDERSON,THOMAS,JACKSON,WHITE,HARRIS,MARTIN,THOMPSON,GARCIA,MARTINEZ,ROBINSON,CLARK,RODRIGUEZ,LEWIS,LEE,WALKER,HALL,ALLEN,YOUNG,HERNANDEZ,KING,WRIGHT,LOPEZ,HILL,SCOTT,GREEN,ADAMS,BAKER,GONZALEZ,NELSON,CARTER,MITCHELL,PEREZ,ROBERTS,TURNER,PHILLIPS,CAMPBELL,PARKER,EVANS,EDWARDS,COLLINS,STEWART,SANCHEZ,MORRIS,ROGERS,REED,COOK,MORGAN,BELL,MURPHY,BAILEY,RIVERA,COOPER,RICHARDSON,COX,HOWARD,WARD,TORRES,PETERSON,GRAY,RAMIREZ,JAMES,WATSON,BROOKS,KELLY,SANDERS,PRICE,BENNETT,WOOD,BARNES,ROSS,HENDERSON,COLEMAN,JENKINS,PERRY,POWELL,LONG,PATTERSON,HUGHES,FLORES,WASHINGTON,BUTLER,SIMMONS,FOSTER,GONZALES,BRYANT,ALEXANDER,RUSSELL,GRIFFIN,DIAZ,HAYES,MYERS,FORD,HAMILTON,GRAHAM,SULLIVAN,WALLACE,WOODS,COLE,WEST,JORDAN,OWENS,REYNOLDS,FISHER,ELLIS,HARRISON,GIBSON,MCDONALD,CRUZ,MARSHALL,ORTIZ,GOMEZ,MURRAY,FREEMAN,WELLS,WEBB,SIMPSON,STEVENS,TUCKER,PORTER,HUNTER,HICKS,CRAWFORD,HENRY,BOYD,MASON,MORALES,KENNEDY,WARREN,DIXON,RAMOS,REYES,BURNS,GORDON,SHAW,HOLMES,RICE,ROBERTSON,HUNT,BLACK,DANIELS,PALMER,MILLS,NICHOLS,GRANT,KNIGHT,FERGUSON,ROSE,STONE,HAWKINS,DUNN,PERKINS,HUDSON,SPENCER,GARDNER,STEPHENS,PAYNE,PIERCE,BERRY,MATTHEWS,ARNOLD,WAGNER,WILLIS,RAY,WATKINS,OLSON,CARROLL,DUNCAN,SNYDER,HART,CUNNINGHAM,BRADLEY,LANE,ANDREWS,RUIZ,HARPER,FOX,RILEY,ARMSTRONG,CARPENTER,WEAVER,GREENE,LAWRENCE,ELLIOTT,CHAVEZ,SIMS,AUSTIN,PETERS,KELLEY,FRANKLIN,LAWSON,FIELDS,GUTIERREZ,RYAN,SCHMIDT,CARR,VASQUEZ,CASTILLO,WHEELER,CHAPMAN,OLIVER,MONTGOMERY,RICHARDS,WILLIAMSON,JOHNSTON,BANKS,MEYER,BISHOP,MCCOY,HOWELL,ALVAREZ,MORRISON,HANSEN,FERNANDEZ,GARZA,HARVEY,LITTLE,BURTON,STANLEY,NGUYEN,GEORGE,JACOBS,REID,KIM,FULLER,LYNCH,DEAN,GILBERT,GARRETT,ROMERO,WELCH,LARSON,FRAZIER,BURKE,HANSON,DAY,MENDOZA,MORENO,BOWMAN,MEDINA,FOWLER,BREWER,HOFFMAN,CARLSON,SILVA,PEARSON,HOLLAND,DOUGLAS,FLEMING,JENSEN,VARGAS,BYRD,DAVIDSON,HOPKINS,MAY,TERRY,HERRERA,WADE,SOTO,WALTERS,CURTIS,NEAL,CALDWELL,LOWE,JENNINGS,BARNETT,GRAVES,JIMENEZ,HORTON,SHELTON,BARRETT,OBRIEN,CASTRO,SUTTON,GREGORY,MCKINNEY,LUCAS,MILES,CRAIG,RODRIQUEZ,CHAMBERS,HOLT,LAMBERT,FLETCHER,WATTS,BATES,HALE,RHODES,PENA,BECK,NEWMAN,HAYNES,MCDANIEL,MENDEZ,BUSH,VAUGHN,PARKS,DAWSON,SANTIAGO,NORRIS,HARDY,LOVE,STEELE,CURRY,POWERS,SCHULTZ,BARKER,GUZMAN,PAGE,MUNOZ,BALL,KELLER,CHANDLER,WEBER,LEONARD,WALSH,LYONS,RAMSEY,WOLFE,SCHNEIDER,MULLINS,BENSON,SHARP,BOWEN,DANIEL,BARBER,CUMMINGS,HINES,BALDWIN,GRIFFITH,VALDEZ,HUBBARD,SALAZAR,REEVES,WARNER,STEVENSON,BURGESS,SANTOS,TATE,CROSS,GARNER,MANN,MACK,MOSS,THORNTON,DENNIS,MCGEE,FARMER,DELGADO,AGUILAR,VEGA,GLOVER,MANNING,COHEN,HARMON,RODGERS,ROBBINS,NEWTON,TODD,BLAIR,HIGGINS,INGRAM,REESE,CANNON,STRICKLAND,TOWNSEND,POTTER,GOODWIN,WALTON,ROWE,HAMPTON,ORTEGA,PATTON,SWANSON,JOSEPH,FRANCIS,GOODMAN,MALDONADO,YATES,BECKER,ERICKSON,HODGES,RIOS,CONNER,ADKINS,WEBSTER,NORMAN,MALONE,HAMMOND,FLOWERS,COBB,MOODY,QUINN,BLAKE,MAXWELL,POPE,FLOYD,OSBORNE,PAUL,MCCARTHY,GUERRERO,LINDSEY,ESTRADA,SANDOVAL,GIBBS,TYLER,GROSS,FITZGERALD,STOKES,DOYLE,SHERMAN,SAUNDERS,WISE,COLON,GILL,ALVARADO,GREER,PADILLA,SIMON,WATERS,NUNEZ,BALLARD,SCHWARTZ,MCBRIDE,HOUSTON,CHRISTENSEN,KLEIN,PRATT,BRIGGS,PARSONS,MCLAUGHLIN,ZIMMERMAN,FRENCH,BUCHANAN,MORAN,COPELAND,ROY,PITTMAN,BRADY,MCCORMICK,HOLLOWAY,BROCK,POOLE,FRANK,LOGAN,OWEN,BASS,MARSH,DRAKE,WONG,JEFFERSON,PARK,MORTON,ABBOTT,SPARKS,PATRICK,NORTON,HUFF,CLAYTON,MASSEY,LLOYD,FIGUEROA,CARSON,BOWERS,ROBERSON,BARTON,TRAN,LAMB,HARRINGTON,CASEY,BOONE,CORTEZ,CLARKE,MATHIS,SINGLETON,WILKINS,CAIN,BRYAN,UNDERWOOD,HOGAN,MCKENZIE,COLLIER,LUNA,PHELPS,MCGUIRE,ALLISON,BRIDGES,WILKERSON,NASH,SUMMERS,ATKINS,WILCOX,PITTS,CONLEY,MARQUEZ,BURNETT,RICHARD,COCHRAN,CHASE,DAVENPORT,HOOD,GATES,CLAY,AYALA,SAWYER,ROMAN,VAZQUEZ,DICKERSON,HODGE,ACOSTA,FLYNN,ESPINOZA,NICHOLSON,MONROE,WOLF,MORROW,KIRK,RANDALL,ANTHONY,WHITAKER,OCONNOR,SKINNER,WARE,MOLINA,KIRBY,HUFFMAN,BRADFORD,CHARLES,GILMORE,DOMINGUEZ,ONEAL,BRUCE,LANG,COMBS,KRAMER,HEATH,HANCOCK,GALLAGHER,GAINES,SHAFFER,SHORT,WIGGINS,MATHEWS,MCCLAIN,FISCHER,WALL,SMALL,MELTON,HENSLEY,BOND,DYER,CAMERON,GRIMES,CONTRERAS,CHRISTIAN,WYATT,BAXTER,SNOW,MOSLEY,SHEPHERD,LARSEN,HOOVER,BEASLEY,GLENN,PETERSEN,WHITEHEAD,MEYERS,KEITH,GARRISON,VINCENT,SHIELDS,HORN,SAVAGE,OLSEN,SCHROEDER,HARTMAN,WOODARD,MUELLER,KEMP,DELEON,BOOTH,PATEL,CALHOUN,WILEY,EATON,CLINE,NAVARRO,HARRELL,LESTER,HUMPHREY,PARRISH,DURAN,HUTCHINSON,HESS,DORSEY,BULLOCK,ROBLES,BEARD,DALTON,AVILA,VANCE,RICH,BLACKWELL,YORK,JOHNS,BLANKENSHIP,TREVINO,SALINAS,CAMPOS,PRUITT,MOSES,CALLAHAN,GOLDEN,MONTOYA,HARDIN,GUERRA,MCDOWELL,CAREY,STAFFORD,GALLEGOS,HENSON,WILKINSON,BOOKER,MERRITT,MIRANDA,ATKINSON,ORR,DECKER,HOBBS,PRESTON,TANNER,KNOX,PACHECO,STEPHENSON,GLASS,ROJAS,SERRANO,MARKS,HICKMAN,ENGLISH,SWEENEY,STRONG,PRINCE,MCCLURE,CONWAY,WALTER,ROTH,MAYNARD,FARRELL,LOWERY,HURST,NIXON,WEISS,TRUJILLO,ELLISON,SLOAN,JUAREZ,WINTERS,MCLEAN,RANDOLPH,LEON,BOYER,VILLARREAL,MCCALL,GENTRY,CARRILLO,KENT,AYERS,LARA,SHANNON,SEXTON,PACE,HULL,LEBLANC,BROWNING,VELASQUEZ,LEACH,CHANG,HOUSE,SELLERS,HERRING,NOBLE,FOLEY,BARTLETT,MERCADO,LANDRY,DURHAM,WALLS,BARR,MCKEE,BAUER,RIVERS,EVERETT,BRADSHAW,PUGH,VELEZ,RUSH,ESTES,DODSON,MORSE,SHEPPARD,WEEKS,CAMACHO,BEAN,BARRON,LIVINGSTON,MIDDLETON,SPEARS,BRANCH,BLEVINS,CHEN,KERR,MCCONNELL,HATFIELD,HARDING,ASHLEY,SOLIS,HERMAN,FROST,GILES,BLACKBURN,WILLIAM,PENNINGTON,WOODWARD,FINLEY,MCINTOSH,KOCH,BEST,SOLOMON,MCCULLOUGH,DUDLEY,NOLAN,BLANCHARD,RIVAS,BRENNAN,MEJIA,KANE,BENTON,JOYCE,BUCKLEY,HALEY,VALENTINE,MADDOX,RUSSO,MCKNIGHT,BUCK,MOON,MCMILLAN,CROSBY,BERG,DOTSON,MAYS,ROACH,CHURCH,CHAN,RICHMOND,MEADOWS,FAULKNER,ONEILL,KNAPP,KLINE,BARRY,OCHOA,JACOBSON,GAY,AVERY,HENDRICKS,HORNE,SHEPARD,HEBERT,CHERRY,CARDENAS,MCINTYRE,WHITNEY,WALLER,HOLMAN,DONALDSON,CANTU,TERRELL,MORIN,GILLESPIE,FUENTES,TILLMAN,SANFORD,BENTLEY,PECK,KEY,SALAS,ROLLINS,GAMBLE,DICKSON,BATTLE,SANTANA,CABRERA,CERVANTES,HOWE,HINTON,HURLEY,SPENCE,ZAMORA,YANG,MCNEIL,SUAREZ,CASE,PETTY,GOULD,MCFARLAND,SAMPSON,CARVER,BRAY,ROSARIO,MACDONALD,STOUT,HESTER,MELENDEZ,DILLON,FARLEY,HOPPER,GALLOWAY,POTTS,BERNARD,JOYNER,STEIN,AGUIRRE,OSBORN,MERCER,BENDER,FRANCO,ROWLAND,SYKES,BENJAMIN,TRAVIS,PICKETT,CRANE,SEARS,MAYO,DUNLAP,HAYDEN,WILDER,MCKAY,COFFEY,MCCARTY,EWING,COOLEY,VAUGHAN,BONNER,COTTON,HOLDER,STARK,FERRELL,CANTRELL,FULTON,LYNN,LOTT,CALDERON,ROSA,POLLARD,HOOPER,BURCH,MULLEN,FRY,RIDDLE,LEVY,DAVID,DUKE,ODONNELL,GUY,MICHAEL,BRITT,FREDERICK,DAUGHERTY,BERGER,DILLARD,ALSTON,JARVIS,FRYE,RIGGS,CHANEY,ODOM,DUFFY,FITZPATRICK,VALENZUELA,MERRILL,MAYER,ALFORD,MCPHERSON,ACEVEDO,DONOVAN,BARRERA,ALBERT,COTE,REILLY,COMPTON,RAYMOND,MOONEY,MCGOWAN,CRAFT,CLEVELAND,CLEMONS,WYNN,NIELSEN,BAIRD,STANTON,SNIDER,ROSALES,BRIGHT,WITT,STUART,HAYS,HOLDEN,RUTLEDGE,KINNEY,CLEMENTS,CASTANEDA,SLATER,HAHN,EMERSON,CONRAD,BURKS,DELANEY,PATE,LANCASTER,SWEET,JUSTICE,TYSON,SHARPE,WHITFIELD,TALLEY,MACIAS,IRWIN,BURRIS,RATLIFF,MCCRAY,MADDEN,KAUFMAN,BEACH,GOFF,CASH,BOLTON,MCFADDEN,LEVINE,GOOD,BYERS,KIRKLAND,KIDD,WORKMAN,CARNEY,DALE,MCLEOD,HOLCOMB,ENGLAND,FINCH,HEAD,BURT,HENDRIX,SOSA,HANEY,FRANKS,SARGENT,NIEVES,DOWNS,RASMUSSEN,BIRD,HEWITT,LINDSAY,LE,FOREMAN,VALENCIA,ONEIL,DELACRUZ,VINSON,DEJESUS,HYDE,FORBES,GILLIAM,GUTHRIE,WOOTEN,HUBER,BARLOW,BOYLE,MCMAHON,BUCKNER,ROCHA,PUCKETT,LANGLEY,KNOWLES,COOKE,VELAZQUEZ,WHITLEY,NOEL,VANG"
# list of 300/300 most popular male/female names in USA
First_names = "JAMES,JOHN,ROBERT,MICHAEL,WILLIAM,DAVID,RICHARD,CHARLES,JOSEPH,THOMAS,CHRISTOPHER,DANIEL,PAUL,MARK,DONALD,GEORGE,KENNETH,STEVEN,EDWARD,BRIAN,RONALD,ANTHONY,KEVIN,JASON,MATTHEW,GARY,TIMOTHY,JOSE,LARRY,JEFFREY,FRANK,SCOTT,ERIC,STEPHEN,ANDREW,RAYMOND,GREGORY,JOSHUA,JERRY,DENNIS,WALTER,PATRICK,PETER,HAROLD,DOUGLAS,HENRY,CARL,ARTHUR,RYAN,ROGER,JOE,JUAN,JACK,ALBERT,JONATHAN,JUSTIN,TERRY,GERALD,KEITH,SAMUEL,WILLIE,RALPH,LAWRENCE,NICHOLAS,ROY,BENJAMIN,BRUCE,BRANDON,ADAM,HARRY,FRED,WAYNE,BILLY,STEVE,LOUIS,JEREMY,AARON,RANDY,HOWARD,EUGENE,CARLOS,RUSSELL,BOBBY,VICTOR,MARTIN,ERNEST,PHILLIP,TODD,JESSE,CRAIG,ALAN,SHAWN,CLARENCE,SEAN,PHILIP,CHRIS,JOHNNY,EARL,JIMMY,ANTONIO,DANNY,BRYAN,TONY,LUIS,MIKE,STANLEY,LEONARD,NATHAN,DALE,MANUEL,RODNEY,CURTIS,NORMAN,ALLEN,MARVIN,VINCENT,GLENN,JEFFERY,TRAVIS,JEFF,CHAD,JACOB,LEE,MELVIN,ALFRED,KYLE,FRANCIS,BRADLEY,JESUS,HERBERT,FREDERICK,RAY,JOEL,EDWIN,DON,EDDIE,RICKY,TROY,RANDALL,BARRY,ALEXANDER,BERNARD,MARIO,LEROY,FRANCISCO,MARCUS,MICHEAL,THEODORE,CLIFFORD,MIGUEL,OSCAR,JAY,JIM,TOM,CALVIN,ALEX,JON,RONNIE,BILL,LLOYD,TOMMY,LEON,DEREK,WARREN,DARRELL,JEROME,FLOYD,LEO,ALVIN,TIM,WESLEY,GORDON,DEAN,GREG,JORGE,DUSTIN,PEDRO,DERRICK,DAN,LEWIS,ZACHARY,COREY,HERMAN,MAURICE,VERNON,ROBERTO,CLYDE,GLEN,HECTOR,SHANE,RICARDO,SAM,RICK,LESTER,BRENT,RAMON,CHARLIE,TYLER,GILBERT,GENE,MARC,REGINALD,RUBEN,BRETT,ANGEL,NATHANIEL,RAFAEL,LESLIE,EDGAR,MILTON,RAUL,BEN,CHESTER,CECIL,DUANE,FRANKLIN,ANDRE,ELMER,BRAD,GABRIEL,RON,MITCHELL,ROLAND,ARNOLD,HARVEY,JARED,ADRIAN,KARL,CORY,CLAUDE,ERIK,DARRYL,JAMIE,NEIL,JESSIE,CHRISTIAN,JAVIER,FERNANDO,CLINTON,TED,MATHEW,TYRONE,DARREN,LONNIE,LANCE,CODY,JULIO,KELLY,KURT,ALLAN,NELSON,GUY,CLAYTON,HUGH,MAX,DWAYNE,DWIGHT,ARMANDO,FELIX,JIMMIE,EVERETT,JORDAN,IAN,WALLACE,KEN,BOB,JAIME,CASEY,ALFREDO,ALBERTO,DAVE,IVAN,JOHNNIE,SIDNEY,BYRON,JULIAN,ISAAC,MORRIS,CLIFTON,WILLARD,DARYL,ROSS,VIRGIL,ANDY,MARSHALL,SALVADOR,PERRY,KIRK,SERGIO,MARION,TRACY,SETH,KENT,TERRANCE,RENE,EDUARDO,TERRENCE,ENRIQUE,FREDDIE,WADE,MARY,PATRICIA,LINDA,BARBARA,ELIZABETH,JENNIFER,MARIA,SUSAN,MARGARET,DOROTHY,LISA,NANCY,KAREN,BETTY,HELEN,SANDRA,DONNA,CAROL,RUTH,SHARON,MICHELLE,LAURA,SARAH,KIMBERLY,DEBORAH,JESSICA,SHIRLEY,CYNTHIA,ANGELA,MELISSA,BRENDA,AMY,ANNA,REBECCA,VIRGINIA,KATHLEEN,PAMELA,MARTHA,DEBRA,AMANDA,STEPHANIE,CAROLYN,CHRISTINE,MARIE,JANET,CATHERINE,FRANCES,ANN,JOYCE,DIANE,ALICE,JULIE,HEATHER,TERESA,DORIS,GLORIA,EVELYN,JEAN,CHERYL,MILDRED,KATHERINE,JOAN,ASHLEY,JUDITH,ROSE,JANICE,KELLY,NICOLE,JUDY,CHRISTINA,KATHY,THERESA,BEVERLY,DENISE,TAMMY,IRENE,JANE,LORI,RACHEL,MARILYN,ANDREA,KATHRYN,LOUISE,SARA,ANNE,JACQUELINE,WANDA,BONNIE,JULIA,RUBY,LOIS,TINA,PHYLLIS,NORMA,PAULA,DIANA,ANNIE,LILLIAN,EMILY,ROBIN,PEGGY,CRYSTAL,GLADYS,RITA,DAWN,CONNIE,FLORENCE,TRACY,EDNA,TIFFANY,CARMEN,ROSA,CINDY,GRACE,WENDY,VICTORIA,EDITH,KIM,SHERRY,SYLVIA,JOSEPHINE,THELMA,SHANNON,SHEILA,ETHEL,ELLEN,ELAINE,MARJORIE,CARRIE,CHARLOTTE,MONICA,ESTHER,PAULINE,EMMA,JUANITA,ANITA,RHONDA,HAZEL,AMBER,EVA,DEBBIE,APRIL,LESLIE,CLARA,LUCILLE,JAMIE,JOANNE,ELEANOR,VALERIE,DANIELLE,MEGAN,ALICIA,SUZANNE,MICHELE,GAIL,BERTHA,DARLENE,VERONICA,JILL,ERIN,GERALDINE,LAUREN,CATHY,JOANN,LORRAINE,LYNN,SALLY,REGINA,ERICA,BEATRICE,DOLORES,BERNICE,AUDREY,YVONNE,ANNETTE,JUNE,SAMANTHA,MARION,DANA,STACY,ANA,RENEE,IDA,VIVIAN,ROBERTA,HOLLY,BRITTANY,MELANIE,LORETTA,YOLANDA,JEANETTE,LAURIE,KATIE,KRISTEN,VANESSA,ALMA,SUE,ELSIE,BETH,JEANNE,VICKI,CARLA,TARA,ROSEMARY,EILEEN,TERRI,GERTRUDE,LUCY,TONYA,ELLA,STACEY,WILMA,GINA,KRISTIN,JESSIE,NATALIE,AGNES,VERA,WILLIE,CHARLENE,BESSIE,DELORES,MELINDA,PEARL,ARLENE,MAUREEN,COLLEEN,ALLISON,TAMARA,JOY,GEORGIA,CONSTANCE,LILLIE,CLAUDIA,JACKIE,MARCIA,TANYA,NELLIE,MINNIE,MARLENE,HEIDI,GLENDA,LYDIA,VIOLA,COURTNEY,MARIAN,STELLA,CAROLINE,DORA,JO,VICKIE,MATTIE,TERRY,MAXINE,IRMA,MABEL,MARSHA,MYRTLE,LENA,CHRISTY,DEANNA,PATSY,HILDA,GWENDOLYN,JENNIE,NORA,MARGIE,NINA,CASSANDRA,LEAH,PENNY,KAY,PRISCILLA,NAOMI,CAROLE,BRANDY,OLGA,BILLIE,DIANNE,TRACEY,LEONA,JENNY,FELICIA,SONIA,MIRIAM,VELMA,BECKY,BOBBIE,VIOLET,KRISTINA,TONI,MISTY,MAE,SHELLY,DAISY,RAMONA,SHERRI,ERIKA,KATRINA,CLAIRE"

cities = cities.split(",")
cities.sort()
counties = counties.replace("-", " ").split(",")
counties.sort()
borough = borough.split(",")
borough.sort()
borough_ab = borough_ab.split(",")
borough_ab.sort()
school_levels = school_levels.split(",")
school_levels.sort()
park_types = park_types.split(",")
park_types.sort()
colors = colors.split(",")
colors.sort()
business_pattern = business_pattern.split(",")
business_pattern.sort()
school_name_pattern = school_name_pattern.split(",")
school_name_pattern.sort()
neighborhoods = neighborhoods.split(",")
neighborhoods.sort()
street_pattern = street_pattern.split(",")
street_pattern.sort()
color_ab = color_ab.split(",")
color_ab.sort()
car_types = car_types.split(",")
car_types.sort()
building_classification_pattern = building_classification_pattern.split(",")
building_classification_pattern.sort()
subjects = subjects.split(",")
subjects.sort()
study_areas = study_areas.split(",")
study_areas.sort()
location_types = location_types.split(",")
location_types = list(set(location_types) - set(subjects) - set(study_areas) - set(car_types))
location_types.sort()
agencies = agencies.split(",")
agencies.sort()
car_make = car_make.split(",")
car_make.sort()
Last_names = Last_names.split(",")
Last_names = list(set(Last_names) - set(car_make))
Last_names.sort()
First_names = First_names.split(",")
First_names = list(set(First_names) - set(car_make))
First_names.sort()


def binary_search(source_list, target):
    left = 0
    right = len(source_list) - 1
    while left <= right:
        mid = left + (right - left) // 2
        if source_list[mid] == target:
            return True
        elif source_list[mid] < target:
            left = mid + 1
        else:
            right = mid - 1
    return False


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)


def get_type_from_col_name(x):
    col = str(x).lower()
    # Person Name
    if ('first' in col or 'last' in col or 'middle' in col or 'full' in col) and 'name' in col:
        return 'person_name'
    # Business Name
    if ('business' in col and 'name' in col) or 'dba' in col:
        return 'business_name'
    # Phone Number
    if 'phone' in col or 'telephone' in col:
        return 'phone_number'
    # Address
    if 'address' in col:
        return 'address'
    # Street Name
    if 'street' in col:
        return 'street_name'
    # City Agency
    if 'agency' in col:
        return 'city_agency'
    # City
    if 'city' in col and 'agency' not in col:
        return 'city'
    # Neighborhood
    if 'neighborhood' in col:
        return 'neighborhood'
    # LAT/LON coordinates
    if ('lat' in col and 'lon' in col) or 'location' in col:
        return 'lat_lon_cord'
    # Zip code
    if 'zip' in col:
        return 'zip_code'
    # Borough
    if 'boro' in col or 'borough' in col:
        return 'borough'
    # School name
    if col == 'school' or ('school' in col and 'name' in col):
        return 'school_name'
    # Color
    if 'color' in col:
        return 'color'
    # Car Make
    if 'make' in col:
        return 'car_make'
    # Area of study
    if 'interest' in col:
        return 'area_of_study'
    # subjects in school
    if 'subject' in col:
        return 'subject_in_school'
    # school level
    if 'school' in col and 'level' in col:
        return 'school_level'
    # college/university names
    if 'college' in col or 'university' in col:
        return 'college_name'
    # websites
    if 'website' in col or 'site' in col:
        return 'website'
    # building classification
    if 'building' in col and 'classification' in col:
        return 'building_classification'
    # vehicle type
    if 'vehicle' in col and 'type' in col:
        return 'vehicle_type'
    # type of location
    if 'prem' in col and 'typ' in col:
        return 'location_type'
    # parks/playground
    if 'park' in col and 'name' in col:
        return 'park_playground'
    return 'other'


def check_semantic_type(input):
    predict_types = []
    # check null
    if input is None:
        predict_types.append(('other', 1))
        return predict_types
    x = str(input[0]).strip().lower()
    # Regular Expression
    # Phone Number
    if is_phone(x):
        predict_types.append(('phone_number', input[1]))
    # LAT/LON coordinates
    if is_long_lat(x):
        predict_types.append(('lat_lon_cord', input[1]))
    # Zip code
    if is_zip(x):
        predict_types.append(('zip_code', input[1]))
    # websites
    if is_website(x):
        predict_types.append(('website', input[1]))
    # External Check (small)
    # building classification
    if is_building_classification(x):
        predict_types.append(('building_classification', input[1]))
    # School name
    if is_school_name(x):
        predict_types.append(('school_name', input[1]))
    # Borough
    if is_borough(x):
        predict_types.append(('borough', input[1]))
    # school level
    if is_school_level(x):
        predict_types.append(('school_level', input[1]))
    # subjects in school
    if is_subject_in_school(x):
        predict_types.append(('subject_in_school', input[1]))
    # Area of study
    if is_area_of_study(x):
        predict_types.append(('area_of_study', input[1]))
    # parks/playground
    if is_park_playground(x):
        predict_types.append(('park_playground', input[1]))
    # Business Name
    if is_business_name(x):
        predict_types.append(('business_name', input[1]))
    # Address
    if is_address(x):
        predict_types.append(('address', input[1]))
    # Street Name
    if is_street(x):
        predict_types.append(('street_name', input[1]))
    # External Check (large)
    # Neighborhood
    if is_neighborhood(x):
        predict_types.append(('neighborhood', input[1]))
    # Color
    if is_color(x):
        predict_types.append(('color', input[1]))
    # City
    if is_city(x):
        predict_types.append(('city', input[1]))
    # Car Make
    if is_car_make(x):
        predict_types.append(('car_make', input[1]))
    # City Agency
    if is_city_agency(x):
        predict_types.append(('city_agency', input[1]))
    # college/university names
    # It seems there are no such names in our datasets (not a single column is manually labeled as such)
    # vehicle type
    if is_vehicle_type(x):
        predict_types.append(('vehicle_type', input[1]))
    # type of location
    if is_type_location(x):
        predict_types.append(('location_type', input[1]))
    # Person Name
    if is_person_name(x):
        predict_types.append(('person_name', input[1]))
    # If none of them
    if len(predict_types) == 0:
        predict_types.append(('other', input[1]))
    return predict_types


def is_long_lat(x):
    return re.match(
        re.compile(r'^[(]?[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)[)]?$'),
        x)


def is_city_agency(x):
    return binary_search(agencies, x.upper())


def is_school_level(x):
    return binary_search(school_levels, x)


def is_car_make(x):
    return binary_search(car_make, x.upper())


def is_school_name(x):
    if is_school_level(x):
        return False
    x = x.split()
    for e in x:
        if binary_search(school_name_pattern, e):
            return True
    return False


def is_vehicle_type(x):
    return binary_search(car_types, x)


def is_type_location(x):
    return binary_search(location_types, x)


# decide if this string is a street_name or an address
def is_address_helper(x):
    int_pattern = re.compile(r'^\d+[-]?\d*$')
    if len(x) >= 3 and re.match(int_pattern, x[0]) and not binary_search(street_pattern, x[1]):
        return True
    else:
        return False


# check if the input string contains a street_name pattern
def is_street_helper(x):
    for e in x:
        if binary_search(street_pattern, e):
            return True
    return False


def is_street(x):
    x = x.split()
    return is_street_helper(x) and not is_address_helper(x)


def is_address(x):
    x = x.split()
    return is_street_helper(x) and is_address_helper(x)


def is_color(x):
    return binary_search(colors, x) or binary_search(color_ab, x.upper())


def is_neighborhood(x):
    return binary_search(neighborhoods, x)


def is_zip(x):
    return re.match(re.compile(r'^[\d]{5,5}$'), x)


def is_person_name(x):
    return binary_search(Last_names, x.upper()) or binary_search(First_names, x.upper())


def is_city(x):
    return binary_search(cities, x) or binary_search(counties, x)


def is_park_playground(x):
    # if string x contains any type of park type string, return true, else false
    for e in park_types:
        if e in x:
            return True
    return False


def is_business_name(x):
    # if string x contains any type of business pattern, return true, else false
    for e in business_pattern:
        if e in x:
            return True
    return False


def is_borough(x):
    return binary_search(borough, x) or binary_search(borough_ab, x.upper())


def is_phone(x):
    x = x.replace('(', "").replace(")", "").replace("-", "").replace(" ", "")
    return re.match(re.compile(r'^[\d]{10,10}$'), x)


def is_website(x):
    pattern = re.compile(
        r'https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,}')
    return re.match(pattern, x)


def is_building_classification(x):
    for e in building_classification_pattern:
        if e in x:
            return True
    return False


def is_subject_in_school(x):
    for e in subjects:
        if e in x:
            return True
    return False


def is_area_of_study(x):
    return binary_search(study_areas, x)


def init_files():
    with open('cluster2.txt') as file:
        origins = [file_name.strip()[1:-1] for file_name in file.readline().split(",")]
    return list(set(origins))


def get_predicted_labels(column_name_type, column_data_type, with_column_name=True):
    # check if the label is 'other'
    if column_name_type == 'other' and column_data_type[0][0] == 'other':
        return 'other'
    count_total = 0
    count_without_other = 0
    for type in column_data_type:
        count_total += type[1]
        if type[0] != 'other':
            count_without_other += type[1]
    res = set()
    if with_column_name:
        res.add(column_name_type)
    for type in column_data_type:
        if type[1] >= count_without_other * 0.3 and type[0] != 'other':
            res.add(type[0])
    if len(res) == 0:
        res.add('other')
    return "|".join(list(res))


def form_manual_label_json(file):
    manual_label_dict = dict()
    manual_label_dict["actual_types"] = []
    with open(file, 'r') as label_file:
        for line in label_file:
            if 'column_name,true_label' in line:
                continue
            column_name = line.split(",")[0].split(".")[1]
            manual_labels = line.split(",")[1].strip().split("|")
            cur_dict = dict()
            cur_dict["column_name"] = column_name
            cur_dict["manual_labels"] = []
            for manual_label in manual_labels:
                tmp = dict()
                tmp["semantic_type"] = manual_label
                cur_dict["manual_labels"].append(tmp)
            manual_label_dict["actual_types"].append(cur_dict)
    with open("./result/task2-manual-labels.json", 'w+') as fp:
        json.dump(manual_label_dict, fp, cls=MyEncoder)
    print("manual_label_json saved")


if __name__ == "__main__":
    # load data
    sc = SparkContext()
    mkdir("./task2_data")
    data_dir = "/user/hm74/NYCColumns/"
    files = init_files()
    form_manual_label_json("./result/real_label.csv")
    count = 1
    # init parameters
    result = dict()
    result["predicted_types"] = []
    csv_result_without_column_name = []
    csv_result_with_column_name = []
    for file in files:
        print("-------------------------------------------------------------------------")
        print("file number %s" % count)
        # init parameters and dict
        count += 1
        dataset = file.split(".")[0]
        column = file.split(".")[1]
        column_dict = dict()
        column_dict["column_name"] = column
        column_dict["semantic_types"] = []
        full_file = data_dir + file
        print("%s %s start" % (dataset, column))
        # get column types according to name
        column_name_type = get_type_from_col_name(column)
        file_rdd = sc.textFile(full_file).cache()
        type_rdd = file_rdd.map(lambda x: (x.split("\t")[0], int(x.split("\t")[1]))) \
            .flatMap(check_semantic_type) \
            .reduceByKey(lambda a, b: a + b) \
            .sortBy(lambda x: -x[1]).cache()
        column_data_type = type_rdd.collect()
        # get predicted labels
        predicted_labels_without_column_name = get_predicted_labels(column_name_type, column_data_type,
                                                          with_column_name=False)
        predicted_labels_with_column_name = get_predicted_labels(column_name_type, column_data_type)
        csv_result_without_column_name.append(file.replace(".txt.gz", "") + "," + predicted_labels_without_column_name)
        csv_result_with_column_name.append(file.replace(".txt.gz", "") + "," + predicted_labels_with_column_name)
        # save predicted labels to dict
        for each_type in column_data_type:
            if each_type[0] not in predicted_labels_without_column_name:
                continue
            semantic_type = dict()
            semantic_type['semantic_type'] = each_type[0]
            semantic_type['count'] = each_type[1]
            column_dict["semantic_types"].append(semantic_type)
        result["predicted_types"].append(column_dict)
        # print info
        print(column_name_type)
        print(column_data_type)
        print("without column name: %s" % predicted_labels_without_column_name)
        print("with column name: %s" % predicted_labels_with_column_name)
    # save files
    with open("./result/task2.json", 'w+') as fp:
        json.dump(result, fp, cls=MyEncoder)
    with open("./result/predict_label_with_column_name.csv", 'w+') as fp:
        fp.write("column_name,predict_label_with_column_name\n")
        for line in csv_result_with_column_name:
            fp.write(line + "\n")
    with open("./result/predict_label_without_column_name.csv", 'w+') as fp:
        fp.write("column_name,predict_label_without_column_name\n")
        for line in csv_result_without_column_name:
            fp.write(line + "\n")
