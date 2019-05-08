package repository

import (
	_ "github.com/denisenkom/go-mssqldb"
	"database/sql"
	"net/url"
	"fmt"
	"github.com/certik-project/utils"
)

type TransactionSqlRepository struct {
	config Configuration
}

func NewSqlRepository() TransactionSqlRepository {
	repo := TransactionSqlRepository{}
	repo.config = Configuration {
		"den1.mssql7.gear.host",
		0,
		"certikproject",
		"certik123!",
	}
	return repo
}


type Peer struct {
	IpAddress string
	Port int
}

func (repo TransactionSqlRepository) GetPeers(nodeKey string) []Peer {
	db := repo.getDbConn()
	query := "select IpAddress, Port from Peers\n"
	query += fmt.Sprintf("where NodeKey = '%v'\n", nodeKey)
	query += "order by id desc"

	rows, err := db.Query(query)
	utils.CheckForError(err)
	var peers []Peer
	for rows.Next() {
		var currPeer Peer
		rows.Scan(
			&currPeer.IpAddress,
			&currPeer.Port,
		)
		peers = append(peers, currPeer)
	}
	return peers
}

func (repo TransactionSqlRepository) SavePeer(nodeKey string, ipAddress string, port int) {
	db := repo.getDbConn()
	cmd := fmt.Sprintf(
		"IF NOT EXISTS " +
		"(SELECT 1 FROM Peers " +
		"WHERE IpAddress = '%v' AND Port = %v AND NodeKey = '%v')\n",
			ipAddress, port, nodeKey)
	cmd += "INSERT INTO Peers (NodeKey, IpAddress, Port)\n"
	cmd += fmt.Sprintf("VALUES ('%v','%v',%v)", nodeKey, ipAddress, port)

	defer db.Close() // closing the statement
	_, err := db.Exec(cmd)
	utils.CheckForError(err)
}



// Get the database connection
func (repo TransactionSqlRepository) getDbConn() *sql.DB {
	config := repo.config
	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(config.Username, config.Password),
		Host:	  config.Host,
	}
	conn, err := sql.Open("sqlserver", u.String())
	utils.CheckForError(err)

	return conn
}