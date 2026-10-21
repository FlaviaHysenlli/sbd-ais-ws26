package repository

import (
	"ordersystem/model"
	"time"
)

type DatabaseHandler struct {
	// drinks represent all available drinks
	drinks []model.Drink
	// orders serves as order history
	orders []model.Order
}

// todo
func NewDatabaseHandler() *DatabaseHandler {
	// Init the drinks slice with some test data
	drinks := []model.Drink{
		{ID: 1, Name: "Coffee", Price: 2.99, Description: "Freshly brewed coffee"},
		{ID: 2, Name: "Milkshake", Price: 4.99, Description: "Healthy chocolate milkshake"},
	}

	// Init orders slice with some test data
	orders := []model.Order{
		{DrinkID: 2, CreatedAt: time.Now(), Amount: 2},
	}

	return &DatabaseHandler{
		drinks: drinks,
		orders: orders,
	}
}

func (db *DatabaseHandler) GetDrinks() []model.Drink {
	return db.drinks
}

func (db *DatabaseHandler) GetOrders() []model.Order {
	return db.orders
}

// todo
func (db *DatabaseHandler) GetTotalledOrders() map[uint64]uint64 {
	// calculate total orders
	// key = DrinkID, value = Amount of orders
	// totalledOrders map[uint64]uint64

	totalledOrders := make(map[uint64]uint64)

	for _, order := range db.orders {
		totalledOrders[order.DrinkID] += order.Amount
	}

	return totalledOrders
}

func (db *DatabaseHandler) AddOrder(order *model.Order) {
	// todo
	// add order to db.orders slice
	db.orders = append(db.orders, *order)
}
