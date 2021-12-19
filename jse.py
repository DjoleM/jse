#!/usr/bin/python3

# What we're ignoring:
# - tick sizes
# - daily price limits
# - stock price jump is allowed
# - special quotes
# - Market limits on closing are ignored, closing determined with Itayose

# All orders will be prioritised by price, time then volume assuming
# that all orders are coming from the same security firm in accordance
# to the handbook

from datetime import time as timeType
import heapq
from collections import namedtuple

OrderTuple = namedtuple('orderTuple', 'price time quantity tag')

def formulateOrderTypes(orderStringDict):
    price = int(orderStringDict["Price"])
    quantity = int(orderStringDict["Quantity"])
    time = timeType.fromisoformat(orderStringDict["Time"])
    tag = orderStringDict["Tag"]

    return OrderTuple(price, time, quantity, tag)

def processEobAsk(state, order):
    if (order.price == 0):
        if (order.quantity == 0):
            if(state["marketEobAsks"]):
                removed = state["marketEobAsks"].pop()
                state["marketEobAsksQuantity"] -= removed.quantity
        else:
            state["marketEobAsksQuantity"] += order.quantity
            state["marketEobAsks"].append(order)
    else:
        if(order.quantity == 0):
            newOrders = []
            [ heapq.heappush(newOrders, ask) for ask in state["eobAsks"] if ask.price != order.price ]
            state["eobAsks"] = newOrders
        else:
            heapq.heappush(state["eobAsks"], order)

def processEobBid(state, order):
    if (order.price == 0):
        if (order.quantity == 0):
            if(state["marketEobBids"]):
                removed = state["marketEobBids"].pop()
                state["marketEobBidsQuantity"] -= removed.quantity
        else:
            state["marketEobBidsQuantity"] += order.quantity
            state["marketEobBids"].append(order)
    else:
        if(order.quantity == 0):
            newOrders = []
            [ heapq.heappush(newOrders, bid) for bid in state["eobBids"] if bid.price != order.price * -1 ]
            state["eobBids"] = newOrders
        else:
            heapq.heappush(state["eobBids"], OrderTuple(order.price * -1, order.time, order.quantity, order.tag))

def processAsk(state, order):
    if (order.quantity == 0):
        if(order.price == 0 and state["marketAsks"]):
            removed = state["marketAsks"].pop()
            state["marketAsksQuantity"] -= removed.quantity
        elif(order.price != 0):
            newOrders = []
            [ heapq.heappush(newOrders, ask) for ask in state["asks"] if ask.price != order.price ]
            state["asks"] = newOrders
        return
    if (state["session"] in ["morning auction", "afternoon auction"]):
        if (order.price == 0):
            state["marketAsksQuantity"] += order.quantity
            state["marketAsks"].append(order)
        else:
            heapq.heappush(state["asks"], order)
    else:
        if (order.price == 0):
            zarabaMarketAsk(state, order)
        else:
            zarabaAsk(state, order)

def processBid(state, order):
    if(order.quantity == 0):
        if(order.price == 0 and state["marketAsks"]):
            removed = state["marketBids"].pop()
            state["marketBidsQuantity"] -= removed.quantity
        elif(order.price != 0):
            newOrders = []
            [ heapq.heappush(newOrders, bid) for bid in state["bids"] if bid.price != order.price * -1 ]
            state["bids"] = newOrders
        return
    if (state["session"] in ["morning auction", "afternoon auction"]):
        if (order.price == 0):
            state["marketBids"].append(order)
            state["marketBidsQuantity"] += order.quantity
        else:
            heapq.heappush(state["bids"], OrderTuple(order.price * -1, order.time, order.quantity, order.tag))
    else:
        if (order.price == 0):
            zarabaMarketBid(state, order)
        else:
            zarabaBid(state, order)

def defaultReq(state, order):
    return

def beginSession(state, sessionName):
    itayose(state)
    state["session"] = sessionName

def checkSession(state, time):
    if (time >= timeType.fromisoformat("09:00:00") and time <= timeType.fromisoformat("11:30:00.000000") 
        and state["session"] != "morning session"):
        beginSession(state, "morning session")
    elif (time >= timeType.fromisoformat("11:30:00.000001") and time <= timeType.fromisoformat("12:30:00.000000") 
        and state["session"] != "afternoon auction"):
        state["session"] = "afternoon auction"
    elif (time >= timeType.fromisoformat("12:30:00.000001") and time <= timeType.fromisoformat("15:00:00") 
        and state["session"] != "afternoon session"):
        beginSession(state, "afternoon session")
    return

def readInStocks(state, inputFile):
    commandDict = {
        "QS": processAsk,
        "QB": processBid,
        "SC": processEobAsk,
        "BC": processEobBid
    }
    for i in inputFile:
        orderInfo = {}
        lineData = i.strip().split("|")
        for data in lineData:
            key, value = data.split("=")
            orderInfo[key] = value
        orderInfo = formulateOrderTypes(orderInfo)
        checkSession(state, orderInfo.time)
        commandDict.get(orderInfo.tag, defaultReq)(state, orderInfo)


def makeContinuous(marketQuantity, marketIndex, extendTo, iterable):
    quantities = {}
    currentPrice = marketIndex
    currentQuantity = marketQuantity
    firstPrice = heapq.nsmallest(1, iterable)[0].price
    for price in range(currentPrice, firstPrice):
        quantities[price] = currentQuantity
        currentPrice = price
    while iterable:
        curr = heapq.heappop(iterable)
        cPrice = curr.price
        cQuant = curr.quantity
        for price in range(currentPrice, cPrice):
             quantities[price] = currentQuantity
        currentQuantity += cQuant
        currentPrice = cPrice
        quantities[cPrice] = currentQuantity
    for price in range(currentPrice + 1, extendTo +1):
        quantities[price] = currentQuantity
    return quantities

def makeContinuousNeg(marketQuantity, marketPrice, extendTo, iterable):
    quantities = {}
    currentPrice = marketPrice
    currentQuantity = marketQuantity
    firstPrice = heapq.nlargest(1, iterable)[0].price * -1
    for price in range(currentPrice, firstPrice, -1):
        quantities[price] = currentQuantity
        currentPrice = price
    while iterable:
        curr = heapq.heappop(iterable)
        cPrice = curr.price * -1
        cQuant = curr.quantity
        for price in range(currentPrice, cPrice, -1):
             quantities[price] = currentQuantity
        currentQuantity += cQuant
        currentPrice = cPrice
        quantities[cPrice] = currentQuantity
    for price in range(currentPrice - 1, extendTo -1, -1):
        quantities[price] = currentQuantity
    return quantities

def findRootPrice(bids, asks):
    previous = 0
    for key in sorted(asks.keys()):
        diff = bids[key] - asks[key]
        if (diff < 0):
            return (key, previous)
        previous = key

def checkItayoseConditionsForPrice(continuousBids, continuousAsks, price, marketBids, marketAsks):
    try:
        stepUpBid = continuousBids[price+1]
    except:
        stepUpBid = marketBids
    try:
        stepUpAsk = continuousAsks[price-1]
    except:
        stepUpAsk = marketAsks
    outcome1 = continuousAsks[price] >= continuousBids[price] and stepUpAsk < continuousBids[price]
    outcome2 = continuousAsks[price] <= continuousBids[price] and continuousAsks[price] > stepUpBid
    return outcome1 or outcome2

def tradeItayoseWithPrice(state, price, bidsAtPrice, asksAtPrice):
    tradedQuantity = min(bidsAtPrice, asksAtPrice)
    remainingBids = []
    [ heapq.heappush(remainingBids, b) for b in state["bids"] if b.price * -1 < price ]
    remainingAsks = []
    [ heapq.heappush(remainingAsks, a) for a in state["asks"] if a.price > price ]
    
    if (bidsAtPrice > asksAtPrice):
        remainingOrder = OrderTuple(price * -1, timeType.fromisoformat("00:00:00.000001"), abs(bidsAtPrice - asksAtPrice), "SO")
        heapq.heappush(remainingBids, remainingOrder)
    elif (bidsAtPrice < asksAtPrice):
        remainingOrder = OrderTuple(price, timeType.fromisoformat("00:00:00.000001"), abs(bidsAtPrice - asksAtPrice), "SO")
        heapq.heappush(remainingAsks, remainingOrder)

    state["bids"] = remainingBids
    state["asks"] = remainingAsks
    state["marketAsks"] = []
    state["marketAsksQuantity"] = 0
    state["marketBids"] = []
    state["marketBidsQuantity"] = 0
    state["volumeTraded"] += tradedQuantity
    state["price"] = price
    


def itayose(state):
    minPrice = min(heapq.nlargest(1, state["bids"])[0].price * -1, heapq.nsmallest(1, state["asks"])[0].price)
    maxPrice = max(heapq.nsmallest(1, state["bids"])[0].price * -1, heapq.nlargest(1, state["asks"])[0].price)
    continuousAsks = makeContinuous(state["marketAsksQuantity"], minPrice, maxPrice, state["asks"][:])
    continuousBids = makeContinuousNeg(state["marketBidsQuantity"], maxPrice, minPrice, state["bids"][:])
    # find the exact price where we switch
    rootPrices = findRootPrice(continuousBids, continuousAsks)

    possiblePrices = [rootPrices[0], rootPrices[1]]

    try:
        itayosePrice = [p for p in possiblePrices if checkItayoseConditionsForPrice(continuousBids, continuousAsks, p, state["marketBidsQuantity"], state["marketAsksQuantity"])][0]
    except:
        print("no cross over in price, itayose skipped, no instructions what to do")
        return
    tradeItayoseWithPrice(state, itayosePrice, continuousBids[itayosePrice], continuousAsks[itayosePrice])

def zarabaMarketAsk(state, order):
    # brings a quicker halt to recursion
    if(order.quantity == 0):
        return
    # There's already a queue of market asks, add to it
    if(state["marketAsks"]):
        state["marketAsks"].append(order)
        return
    if (state["marketBids"]):
        bestOffer = state["marketBids"].pop(0)
        state["marketBidsQuantity"] -= bestOffer.quantity
        m = True
    else:
        if (state["bids"]):
            bestOfferOg = heapq.heappop(state["bids"])
            bestOffer = OrderTuple(bestOfferOg.price * -1, bestOfferOg.time, bestOfferOg.quantity, bestOfferOg.tag)
            m = False
        else:
            state["marketAsks"].append(order) 
            return
    if(bestOffer.quantity <= order.quantity):
        state["volumeTraded"] += bestOffer.quantity
        state["price"] = bestOffer.price * -1
        q = order.quantity - bestOffer.quantity
        if (order.quantity > 0):
            zarabaMarketAsk(state, OrderTuple(order.price, order.time, q, order.tag))
    else:
        state["volumeTraded"] += order.quantity
        state["price"] = bestOffer.price
        q = bestOffer.quantity - order.quantity
        if (m):
            state["marketBids"].prepend(OrderTuple(bestOffer.price, bestOffer.time, q, bestOffer.tag))
            state["marketBidsQuantity"] += q
        else:
            heapq.heappush(state["bids"], OrderTuple(bestOffer.price * -1, bestOffer.time, q, bestOffer.tag))

def zarabaAsk(state, order):
    # brings a quicker halt to recursion
    if(order.quantity == 0):
        return
    if (state["asks"] and heapq.nsmallest(1, state["asks"])[0].price < order.price):
        heapq.heappush(state["asks"], order)
        return
    if (state["marketBids"]):
        bestOffer = state["marketBids"].pop(0)
        state["marketBidsQuantity"] -= bestOffer.quantity
        m = True
    else:
        if (state["bids"]):
            bestOfferOg = heapq.heappop(state["bids"])
            bestOffer = OrderTuple(bestOfferOg.price * -1, bestOfferOg.time, bestOfferOg.quantity, bestOfferOg.tag)
            m = False
            if(bestOffer.price < order.price):
                heapq.heappush(state["bids"], bestOfferOg)
                heapq.heappush(state["asks"], order) 
                return
        else:
            heapq.heappush(state["asks"], order) 
            return
    if(bestOffer.quantity <= order.quantity):
        state["volumeTraded"] += bestOffer.quantity
        state["price"] = bestOffer.price
        q = order.quantity - bestOffer.quantity
        if (order.quantity > 0):
            zarabaAsk(state, OrderTuple(order.price, order.time, q, order.tag))
    else:
        state["volumeTraded"] += order.quantity
        state["price"] = bestOffer.price
        q = bestOffer.quantity - order.quantity
        if (m):
            state["marketBids"].prepend(OrderTuple(bestOffer.price, bestOffer.time, q, bestOffer.tag))
            state["marketBidsQuantity"] += q
        else:
            heapq.heappush(state["bids"], OrderTuple(bestOffer.price * -1, bestOffer.time, q, bestOffer.tag))
        
def zarabaMarketBid(state, order):
    # brings a quicker halt to recursion
    if(order.quantity == 0):
        return
    # There's already a queue of market bids, add to it
    if(state["marketBids"]):
        state["marketBids"].append(order)
        return
    if (state["marketAsks"]):
        bestOffer = state["marketAsks"].pop(0)
        state["marketAsksQuantity"] -= bestOffer.quantity
        m = True
    else:
        if (state["asks"]):
            bestOffer = heapq.heappop(state["asks"])
            m = False
        else:
            state["marketBids"].append(order) 
            return
    if(bestOffer.quantity <= order.quantity):
        state["volumeTraded"] += bestOffer.quantity
        state["price"] = bestOffer.price
        q = order.quantity - bestOffer.quantity
        if (order.quantity > 0):
            zarabaMarketBid(state, OrderTuple(order.price, order.time, q, order.tag))
    else:
        state["volumeTraded"] += order.quantity
        state["price"] = bestOffer.price
        q = bestOffer.quantity - order.quantity
        if (m):
            state["marketAsks"].prepend(OrderTuple(bestOffer.price, bestOffer.time, q, bestOffer.tag))
            state["marketAsksQuantity"] += q
        else:
            heapq.heappush(state["asks"], OrderTuple(bestOffer.price, bestOffer.time, q, bestOffer.tag))

def zarabaBid(state, order):
    if (state["bids"] and heapq.nsmallest(1, state["bids"])[0].price * -1 >= order.price):
        heapq.heappush(state["bids"], OrderTuple(order.price * -1, order.time, order.quantity, order.tag))
        return
    if (state["marketAsks"]):
        bestOffer = state["marketAsks"].pop(0)
        state["marketAsksQuantity"] -= bestOffer.quantity
        m = True
    else:
        if (state["asks"]):
            bestOffer = heapq.heappop(state["asks"])
            if(bestOffer.price > order.price):
                #push the best offer back
                heapq.heappush(state["asks"], bestOffer)
                heapq.heappush(state["bids"], OrderTuple(order.price * -1, order.time, order.quantity, order.tag)) 
                return
            m = False
        else:
            # No asks in state, we can't match the order
            heapq.heappush(state["bids"], OrderTuple(order.price * -1, order.time, order.quantity, order.tag)) 
            return
    if(bestOffer.quantity <= order.quantity):
        state["volumeTraded"] += bestOffer.quantity
        state["price"] = bestOffer.price
        q = order.quantity - bestOffer.quantity
        if (order.quantity > 0):
            zarabaBid(state, OrderTuple(order.price, order.time, q, order.tag))
    else:
        state["volumeTraded"] += order.quantity
        state["price"] = bestOffer.price
        q = bestOffer.quantity - order.quantity
        if (m):
            state["marketAsks"].push(OrderTuple(bestOffer.price, bestOffer.time, q, bestOffer.tag))
            state["marketAsksQuantity"] += q
        else:
            heapq.heappush(state["asks"], OrderTuple(bestOffer.price, bestOffer.time, q, bestOffer.tag))

def uncrossEobOrders(state):
    state["marketAsks"] = state["marketAsks"] + state["marketEobAsks"]
    state["marketBids"] = state["marketBids"] + state["marketEobBids"]
    state["marketAsksQuantity"] = state["marketAsksQuantity"] + state["marketEobAsksQuantity"]
    state["marketBidsQuantity"] = state["marketBidsQuantity"] + state["marketEobBidsQuantity"]
    while state["eobAsks"]:
        heapq.heappush(state["asks"], heapq.heappop(state["eobAsks"]))
    while state["eobBids"]:
        heapq.heappush(state["bids"], heapq.heappop(state["eobBids"]))
    state["marketEobAsks"] = []
    state["marketEobBids"] = []
    state["marketEobAsksQuantity"] = 0
    state["marketEobBidsQuantity"] = 0


def processFile(inputFilePath):
    inputFile = open(inputFilePath, 'r')
    state = {
        "bids": [],
        "asks": [],
        "marketAsks": [],
        "marketAsksQuantity": 0,
        "marketBids": [],
        "marketBidsQuantity": 0,
        "eobBids": [],
        "eobAsks": [],
        "marketEobAsks": [],
        "marketEobAsksQuantity": 0,
        "marketEobBids": [],
        "marketEobBidsQuantity": 0,
        "session": "morning auction",
        "volumeTraded": 0,
        "price": 0
    }
    readInStocks(state, inputFile)
    uncrossEobOrders(state)
    itayose(state)


    print("Stock:", inputFilePath)
    print("Closing price:", state["price"])
    print("Volume traded in the day:", state["volumeTraded"])
    print("\n\n\n\n")

inputFiles = [
    "./input/tse_stock1.txt",
    "./input/tse_stock2.txt",
    "./input/tse_stock3.txt",
    "./input/tse_stock4.txt",
    "./input/tse_stock5.txt"
]


for f in inputFiles:
    processFile(f)