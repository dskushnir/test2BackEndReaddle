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
	defer db.Close()
	GetCurrentManager(db)
	GetEmployeeToCongratulate(db)
	GetDepartmentsStatistic(db)

}
func GetCurrentManager(db *sql.DB) {

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
	fmt.Println("______________________________________________________")
}

func GetEmployeeToCongratulate(db *sql.DB) {

	rows, err := db.Query("select dept_name, title, first_name, last_name, hire_date, ceil(( DateDiff(current_date(),employees.hire_date )/365 )) years_Workfrom from employees, dept_manager, departments, titles where employees.emp_no = dept_manager.emp_no and current_date()<dept_manager.to_date and departments.dept_no = dept_manager.dept_no and employees.emp_no = titles.emp_no and current_date()<titles.to_date and  month( employees.hire_date)=month(current_date()) union select dept_name, title, first_name, last_name, hire_date, ceil(( DateDiff(current_date(),employees.hire_date )/365 ))years_Work from employees, dept_emp, departments, titles where employees.emp_no = dept_emp.emp_no and departments.dept_no = dept_emp.dept_no and employees.emp_no = titles.emp_no and current_date()<titles.to_date and  month( employees.hire_date)=month(current_date())")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	employees := make([]*EmployeeToCongratulate, 0)
	for rows.Next() {
		employee := new(EmployeeToCongratulate)
		err := rows.Scan(&employee.DeptName, &employee.Title, &employee.FirstName, &employee.LastName, &employee.HireDate, &employee.YearsWork)
		if err != nil {
			log.Fatal(err)
		}
		employees = append(employees, employee)
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
	for _, employee := range employees {
		fmt.Println("Department:", employee.DeptName, "Title:", employee.Title, "FirstName:",
			employee.FirstName, "LastName:", employee.LastName, "HireDate:", employee.HireDate, "YearsWork:", employee.YearsWork)

	}
	fmt.Println("______________________________________________________")
}
func GetDepartmentsStatistic(db *sql.DB) {
	rows, err := db.Query("select dept_name,count(*),SUM(salary) from employees, dept_manager, departments, titles,salaries where employees.emp_no = dept_manager.emp_no and current_date()<dept_manager.to_date  and departments.dept_no = dept_manager.dept_no and employees.emp_no = titles.emp_no and current_date()<titles.to_date and employees.emp_no=salaries.emp_no and current_date()< salaries.to_date group by dept_name union select dept_name,count(*),SUM(salary) from employees, dept_emp, departments, titles,salaries where employees.emp_no = dept_emp.emp_no  and current_date()<dept_emp.to_date and departments.dept_no = dept_emp.dept_no  and employees.emp_no = titles.emp_no and current_date()<titles.to_date and employees.emp_no=salaries.emp_no and current_date()< salaries.to_date group by dept_name")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	depsStats := make([]*DepartmentsStatistic, 0)
	for rows.Next() {
		depStat := new(DepartmentsStatistic)
		err := rows.Scan(&depStat.DeptName, &depStat.DeptEmployeeCount, &depStat.SalarySum)
		if err != nil {
			log.Fatal(err)
		}
		depsStats = append(depsStats, depStat)
		if err = rows.Err(); err != nil {
			log.Fatal(err)
		}
	}
	for _, depStat := range depsStats {
		fmt.Println("Department:", depStat.DeptName, "DeptEmployeeCount:", depStat.DeptEmployeeCount, "SalarySum:", depStat.SalarySum)

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
type EmployeeToCongratulate struct {
	DeptName  string `db:"dept_name"`
	Title     string `db:"title"`
	FirstName string `db:"first_name"`
	LastName  string `db:"last_name"`
	HireDate  string `db:"from_date"`
	YearsWork int    `db:"years_Work"`
}
type DepartmentsStatistic struct {
	DeptName          string `db:"dept_name"`
	DeptEmployeeCount int    `db:"count(*)"`
	SalarySum         int    `db:"SUM(salary)"`
}
