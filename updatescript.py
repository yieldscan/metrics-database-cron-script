from pymongo import MongoClient
import requests
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from pprint import pprint
import time

print ("\nScript started\n")
load_dotenv()

ogclient = MongoClient(os.getenv("OG_CLIENT_URI"))
newclient = MongoClient(os.getenv("NEW_CLIENT_URI"))

dot_history_url = "https://api.coingecko.com/api/v3/coins/polkadot/history?date="
ksm_history_url = "https://api.coingecko.com/api/v3/coins/kusama/history?date="
dot_ticker_url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=polkadot"
ksm_ticker_url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=kusama"

ys_kusama_url = "https://api-yieldscan.onrender.com/api/kusama/transactions/stats"
ys_polkadot_url = "https://api-yieldscan.onrender.com/api/polkadot/transactions/stats"

ogdb = ogclient.test
newdb = newclient.suryansh


def update_coin_prices(lastUpdated, current_time):
	datetoday = datetime(current_time.year, current_time.month, current_time.day)
	date = datetime(lastUpdated.year, lastUpdated.month, lastUpdated.day)
	
	while date <= current_time:

		nextday = date + timedelta(days=1)
		query = { "$and": [{ "date": { "$gte": date }}, { "date": { "$lt": nextday }}]}

		alreadypresentdot = newdb.dotPriceInUSD.find_one(query)
		if alreadypresentdot is None:
			if date < datetoday:
				dotdata = requests.get(f"{dot_history_url}{date.day}-{date.month}-{date.year}").json()
				dotprice = dotdata["market_data"]["current_price"]["usd"]
			else:
				dotprice = requests.get(dot_ticker_url).json()[0]["current_price"]
			result = newdb.dotPriceInUSD.insert_one({"date": date, "price": dotprice})

		alreadypresentksm = newdb.ksmPriceInUSD.find_one(query)
		if alreadypresentksm is None:
			if date < datetoday:
				ksmdata = requests.get(f"{ksm_history_url}{date.day}-{date.month}-{date.year}").json()
				ksmprice = ksmdata["market_data"]["current_price"]["usd"]
			else:
				ksmprice = requests.get(ksm_ticker_url).json()[0]["current_price"]
			result = newdb.ksmPriceInUSD.insert_one({"date": date, "price": ksmprice})

		date = nextday


def update_transaction_data(lastUpdated):

	ogpolkadottransactiondatas = ogdb.polkadottransactiondatas.find({"createdAt": {"$gt": lastUpdated}})
	for tx in ogpolkadottransactiondatas:

		if newdb.polkadottransactiondatas.find_one({"_id": tx["_id"]}) is None:

			date = datetime(tx["createdAt"].year, tx["createdAt"].month, tx["createdAt"].day)
			nextday = date + timedelta(days=1)
			query = { "$and": [{ "date": { "$gte": date }}, { "date": { "$lt": nextday }}]}
			dotprice = newdb.dotPriceInUSD.find_one(query)["price"]

			if tx["successful"] == True:
				stake = tx["stake"]
				alreadyBonded = tx["alreadyBonded"]
				if stake == alreadyBonded:
					dollarvalue = stake*dotprice
				else:
					dollarvalue = abs(stake-alreadyBonded)*dotprice
			else:
				dollarvalue = 0

			txdict = tx
			txdict["dotPriceInUSD"] = dotprice
			txdict["txDollarValue"] = dollarvalue
			result=newdb.polkadottransactiondatas.insert_one(txdict)

	ogkusamatransactiondatas = ogdb.kusamatransactiondatas.find({"createdAt": {"$gt": lastUpdated}})
	for tx in ogkusamatransactiondatas:

		if newdb.kusamatransactiondatas.find_one({"_id": tx["_id"]}) is None:

			date = datetime(tx["createdAt"].year, tx["createdAt"].month, tx["createdAt"].day)
			nextday = date + timedelta(days=1)
			query = { "$and": [{ "date": { "$gte": date }}, { "date": { "$lt": nextday }}]}
			ksmprice = newdb.ksmPriceInUSD.find_one(query)["price"]

		
			if tx["successful"] == True:
				stake = tx["stake"]
				alreadyBonded = tx["alreadyBonded"]
				if stake == alreadyBonded:
					dollarvalue = stake*ksmprice
				else:
					dollarvalue = abs(stake-alreadyBonded)*ksmprice
			else:
				dollarvalue = 0

			txdict = tx
			txdict["ksmPriceInUSD"] = ksmprice
			txdict["txDollarValue"] = dollarvalue
			result=newdb.kusamatransactiondatas.insert_one(txdict)


def update_stats(current_time):

	date = datetime(current_time.year, current_time.month, current_time.day)
	nextday = date + timedelta(days=1)
	query = { "$and": [{ "date": { "$gte": date }}, { "date": { "$lt": nextday }}]}

	alreadypresentdotstat = newdb.polkadotstats.find_one(query)
	if alreadypresentdotstat is None:
		alreadypresentdotstat = newdb.polkadotstats.find()
		ys_polka_stats = requests.get(ys_polkadot_url).json()
		polkadotTotalAmountCurrentlyManaged = ys_polka_stats["totalAmountCurrentlyManaged"]
		ogpolkastats = ogdb.polkadotnominatorstats.find()
		polkadotPercentAmountCaptured = (polkadotTotalAmountCurrentlyManaged*100)/ogpolkastats[0]["totalAmountStaked"]
		data = {
			"_id": alreadypresentdotstat[0]["_id"],
			"date": current_time, 
			"totalAmountCurrentlyManaged": polkadotTotalAmountCurrentlyManaged, 
			"totalAmountStaked": ogpolkastats[0]["totalAmountStaked"], 
			"percentMarketCaptured": polkadotPercentAmountCaptured
		}
		result = newdb.polkadotstats.update_one({"_id": alreadypresentdotstat[0]["_id"]}, {"$set": data})

	alreadypresentksmstat = newdb.kusamastats.find_one(query)
	if alreadypresentksmstat is None:
		alreadypresentksmstat = newdb.kusamastats.find()
		ys_kusama_stats = requests.get(ys_kusama_url).json()
		kusamaTotalAmountCurrentlyManaged = ys_kusama_stats["totalAmountCurrentlyManaged"]
		ogkusamastats = ogdb.kusamanominatorstats.find()
		kusamaPercentAmountCaptured = (kusamaTotalAmountCurrentlyManaged*100)/ogkusamastats[0]["totalAmountStaked"]
		data = {
			"_id": alreadypresentksmstat[0]["_id"],
			"date": current_time, 
			"totalAmountCurrentlyManaged": kusamaTotalAmountCurrentlyManaged, 
			"totalAmountStaked": ogkusamastats[0]["totalAmountStaked"], 
			"percentMarketCaptured": kusamaPercentAmountCaptured
		}
		result = newdb.kusamastats.update_one({"_id": alreadypresentksmstat[0]["_id"]}, {"$set": data})

def update_last_updated(current_time):

	id_last_update = newdb.lastUpdated.find()[0]["_id"]
	result = newdb.lastUpdated.update_one({"_id": id_last_update}, {"$set": {"lastUpdated": current_time}})

def update_nominations_updated_data(lastUpdated):
	ogpolkadottransactiondatas = ogdb.polkadottransactiondatas.find({"createdAt": {"$gt": lastUpdated}})
	for tx in ogpolkadottransactiondatas:

		if tx["successful"] == True:
			stake = tx["stake"]
			alreadyBonded = tx["alreadyBonded"]
			txdict=tx

			
			if stake == alreadyBonded:
				txdict["volume"] = stake
				if newdb.polkadotnominationsupadteddata.find_one({"_id": txdict["_id"]}) is None:
					result=newdb.polkadotnominationsupadteddata.insert_one(txdict)

			elif stake > alreadyBonded:
				txdict["volume"] = stake-alreadyBonded
				if newdb.polkadotamountstakeddata.find_one({"_id": txdict["_id"]}) is None:
					result=newdb.polkadotamountstakeddata.insert_one(txdict)

			if stake > alreadyBonded or stake < alreadyBonded:
				if newdb.polkadotAUMdata.find_one({"stashId": txdict["stashId"]}) is None:
					txdict["volume"] = stake
				else:
					txdict["volume"] = stake-alreadyBonded
				if newdb.polkadotAUMdata.find_one({"_id": txdict["_id"]}) is None:
					result = newdb.polkadotAUMdata.insert_one(txdict)

			elif stake == alreadyBonded:
				if newdb.polkadotAUMdata.find_one({"stashId": tx["stashId"]}) is None:
					txdict["volume"] = stake
					if newdb.polkadotAUMdata.find_one({"_id": txdict["_id"]}) is None:
						result = newdb.polkadotAUMdata.insert_one(txdict)

	ogkusamatransactiondatas = ogdb.kusamatransactiondatas.find({"createdAt": {"$gt": lastUpdated}})
	for tx in ogkusamatransactiondatas:

		if tx["successful"] == True:
			stake = tx["stake"]
			alreadyBonded = tx["alreadyBonded"]
			txdict=tx

			if stake == alreadyBonded:
				txdict["volume"] = stake
				if newdb.kusamanominationsupadteddata.find_one({"_id": txdict["_id"]}) is None:
					result=newdb.kusamanominationsupadteddata.insert_one(txdict)

			elif stake > alreadyBonded:
				txdict["volume"] = stake-alreadyBonded
				if newdb.kusamanominationsupadteddata.find_one({"_id": txdict["_id"]}) is None:
					result=newdb.kusamanominationsupadteddata.insert_one(txdict)

			if stake > alreadyBonded or stake < alreadyBonded:
				if newdb.kusamaAUMdata.find_one({"stashId": tx["stashId"]}) is None:
					txdict["volume"] = stake
				else:
					txdict["volume"] = stake-alreadyBonded
				if newdb.kusamaAUMdata.find_one({"_id": txdict["_id"]}) is None:
					result = newdb.kusamaAUMdata.insert_one(txdict)

			elif stake == alreadyBonded:
				if newdb.kusamaAUMdata.find_one({"stashId": tx["stashId"]}) is None:
					txdict["volume"] = stake
					if newdb.kusamaAUMdata.find_one({"_id": txdict["_id"]}) is None:
						result = newdb.kusamaAUMdata.insert_one(txdict)


#driver code

lastUpdated = newdb.lastUpdated.find()[0]["lastUpdated"]
current_time = datetime.now()

update_coin_prices(lastUpdated, current_time)
print("Step 1/5: update_coin_prices completed!")

update_transaction_data(lastUpdated)
print("Step 2/5: update_transaction_data completed!")

update_stats(current_time)
print("Step 3/5: update_stats completed!")

update_last_updated(current_time)
print("Step 4/5: update_last_updated completed!")

update_nominations_updated_data(lastUpdated)
print("Step 5/5: update_nominations_updated_data completed!")

print("\nScript completed at " + str(datetime.now()))
