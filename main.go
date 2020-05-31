package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

func main() {
	db, err := sql.Open("mysql", "root"+":"+"16041949"+"@/"+"employees")
	if err != nil {
		log.Fatal(err)
	}
	rows, err := db.Query("select dept_name, title, first_name, last_name, hire_date, salary from employees, dept_manager, departments, titles, salaries where employees.emp_no = dept_manager.emp_no and current_date()<dept_manager.to_date and departments.dept_no = dept_manager.dept_no and employees.emp_no = titles.emp_no and current_date()<titles.to_date and employees.emp_no = salaries.emp_no and current_date()< salaries.to_date")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	managers := make([]*CurrentManager, 0)
	for rows.Next() {
		manager := new(CurrentManager)
		err := rows.Scan(&manager.DeptName, &manager.Title, &manager.FirstName, &manager.LastName, &manager.HireDate, &manager.Salary)
		if err != nil {
			log.Fatal(err)
		}
		managers = append(managers, manager)

	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
	for _, manager := range managers {
		fmt.Println("Department:", manager.DeptName, "Title:", manager.Title, "FirstName:",
			manager.FirstName, "LastName:", manager.LastName, "HireDate:", manager.HireDate, "Salary:", manager.Salary)

	}

}

type CurrentManager struct {
	DeptName  string `db:"dept_name"`
	Title     string `db:"title"`
	FirstName string `db:"first_name"`
	LastName  string `db:"last_name"`
	HireDate  string `db:"from_date"`
	Salary    int    `db:"salary"`
}
