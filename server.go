package main

import (
	"fmt"
	"net"
	"bufio"
	"strconv"
	"strings"
	"time"
	"encoding/base64"
	"log"
	"crypto/md5"
    "encoding/hex"
	"crypto/rand"
	"crypto/hmac"
	"encoding/json"
	"database/sql"
	"github.com/joho/godotenv"
	"os"
    _ "github.com/lib/pq"
)

type Postgres struct {
	Conn *sql.DB
	Host string
	Port int
	DBName string
	User string
	Password string
}

func (p *Postgres) Connect() {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", p.Host, p.Port, p.User, p.Password, p.DBName)
	var err error
    p.Conn, err = sql.Open("postgres", psqlconn)
    if err != nil {
		fmt.Println(err)
	}
 
    defer p.Conn.Close()
}

func (p *Postgres) Exec(statement string) {
    _, err := p.Conn.Exec(statement)
    if err != nil {
		fmt.Println(err)
	}
}

type QueueItem struct {
	Sender string
	Recipients []string
	Data map[string]string
}

type Queue struct {
	Items []QueueItem
}

func (q *Queue) Put(item QueueItem) {
	q.Items = append(q.Items, item)
}

func (q *Queue) Get() QueueItem {
	var next_item QueueItem
	if (len(q.Items) > 0) {
		next_item = q.Items[0]
	}
	if (len(q.Items) > 1) {
		q.Items = q.Items[1:]
	}
	return next_item
}

func (q *Queue) IsEmpty() bool {
	return len(q.Items) == 0
}

func (q *Queue) Size() int {
	return len(q.Items)
}

type Email struct {
	SenderName string
	SenderAddr string
	Recipients []map[string]string
	Cc []map[string]string
	Bcc []map[string]string
	AdditionalHeaders []map[string]string
	Subject string
	Message string
}

func (e *Email) AddRecipient(name string, address string) {
	e.Recipients = append(e.Recipients, map[string]string{"name": name, "address": address})
}

func (e *Email) AddCc(name string, address string) {
	e.Cc = append(e.Cc, map[string]string{"name": name, "address": address})
}

func (e *Email) AddBcc(name string, address string) {
	e.Bcc = append(e.Bcc, map[string]string{"name": name, "address": address})
}

func (e *Email) AddHeader(key string, value string) {
	e.AdditionalHeaders = append(e.AdditionalHeaders, map[string]string{"key": key, "value": value})
}

func (e *Email) ToString() string {
	sender_name := ""
	if (len(e.SenderName) > 0) {
		sender_name = " \""+e.SenderName+"\""
	}
	fmt.Println(sender_name)
	date := time.Now().Format("Mon, 2 Jan 2006 15:04:05 -0700")
	subject := ""
	if (len(e.Subject) > 0) {
		subject = e.Subject
	}
	message := "From:"+sender_name+" <"+e.SenderAddr+">\r\n"
	message += "To:"
	for _, recipient := range e.Recipients {
		recipient_name := ""
		value, ok := recipient["name"]
		if (ok && len(value) > 0) {
			recipient_name = " \""+value+"\""
		}
		message += recipient_name+" <"+recipient["address"]+">,"
	}
	message = message[:len(message)-1]
	message += "\r\n"
	if (len(e.Cc) > 0) {
		message += "Cc:"
		for _, recipient := range e.Cc {
			recipient_name := ""
			value, ok := recipient["name"]
			if (ok && len(value) > 0) {
				recipient_name = " \""+value+"\""
			}
			message += recipient_name+" <"+recipient["address"]+">,"
		}
		message = message[:len(message)-1]
		message += "\r\n"
	}
	if (len(e.Bcc) > 0) {
		message += "Bcc:"
		for _, recipient := range e.Bcc {
			recipient_name := ""
			value, ok := recipient["name"]
			if (ok && len(value) > 0) {
				recipient_name = " \""+value+"\""
			}
			message += recipient_name+" <"+recipient["address"]+">,"
		}
		message = message[:len(message)-1]
		message += "\r\n"
	}
	for _, header := range e.AdditionalHeaders {
		message += header["key"]+": "+header["value"]+"\r\n"
	}
	message += "Date: "+date+"\r\n"
	message += "Subject: "+subject+"\r\n"
	message += e.Message+"\r\n.\r\n"
	return message
}

type SMTPServer struct {
	Domain string
	Host string
	Port int
	Password string
	AuthRequired bool
	MaxMsgSize int
	DebounceNS int64
	Queue Queue
	DB Postgres
}

func (s *SMTPServer) store(sender string, recipient string, data map[string]string) {
	db_data, _ := json.Marshal(data)
	s.DB.Connect()
	s.DB.Exec(fmt.Sprintf(`INSERT INTO mail (sender, recipient, data, created_at) VALUES (%s, %s, %s, %s);`, sender, recipient, db_data, time.Now()))
}

func (s *SMTPServer) processItems() {
	for {
		if (!s.Queue.IsEmpty()) {
			item := s.Queue.Get()
			sender := item.Sender
			recipients := item.Recipients
			data := item.Data
			for _, recipient := range recipients {
				domain := strings.Split(recipient, "@")[1]
				if (domain == s.Domain) {
					s.store(sender, recipient, data)
				} else {
					conn, err := net.Dial("tcp", domain+":587")
					if err != nil {
						fmt.Println(err)
					}
					response, err := bufio.NewReader(conn).ReadString('\n')
					if err != nil {
						fmt.Println(err)
					}
					fmt.Print(response)
				}
			}
		}
	}
}

func (s *SMTPServer) filterData(conn net.Conn, data string) map[string]string {
	if (len(data) > s.MaxMsgSize) {
		fmt.Fprintf(conn, "554 5.3.4 Message too big for system\r\n")
		return nil
	}
	fmt.Print(data)
	filtered := make(map[string]string)
	for _, line := range strings.Split(data,"\r\n") {
		if (strings.HasPrefix(strings.ToLower(line), "date:")) {
			filtered["date"] = line[len("date "):]
		} else if (strings.HasPrefix(strings.ToLower(line), "from:")) {
			if (strings.Index(line, "\"") >= 0) {
				name_start := line[strings.Index(line, "\"")+1:]
				name := name_start[:strings.Index(name_start, "\"")]
				filtered["sender_name"] = name
			}
			filtered["from"] = strings.ToLower(line[strings.Index(line, "<")+1:strings.Index(line, ">")])
		} else if (strings.HasPrefix(strings.ToLower(line), "to:")) {
			if (strings.Index(line, "\"") >= 0) {
				name_start := line[strings.Index(line, "\"")+1:]
				name := name_start[:strings.Index(name_start, "\"")]
				filtered["recipient_name"] = name
			}
			filtered["to"] = strings.ToLower(line[strings.Index(line, "<")+1:strings.Index(line, ">")])
		} else if (strings.HasPrefix(strings.ToLower(line), "subject:")) {
			filtered["subject"] = line[len("subject: "):]
		} else {
			_, ok := filtered["body"]
			if ok {
				filtered["body"] += line
			} else {
				filtered["body"] = line
			}
		}
	}
	return filtered
}

func (s *SMTPServer) authenticateSession(conn net.Conn, is_tls bool, method string) bool {
	method = strings.Trim(strings.ToUpper(method), "\r\n")
	if (method == "PLAIN") {
		if (!is_tls) {
			fmt.Fprintf(conn, "538 5.7.11 Encryption required for requested authentication mechanism\r\n")
			return false
		}
		fmt.Fprintf(conn, "334 \r\n")
		credentials, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
		}
		credentials = strings.Trim(credentials, "\r\n")
		data, err := base64.StdEncoding.DecodeString(credentials)
        if err != nil {
                log.Fatal("error:", err)
        }
		credentials = string(data[:])
		if (credentials == s.Password) {
			fmt.Fprintf(conn, "235 2.7.0 Authentication Succeeded\r\n")
			return true
		}
		fmt.Fprintf(conn, "535 5.7.8 Authentication credentials invalid\r\n")
		return false
	}
	if (method == "DIGEST-MD5") {
		b := make([]byte, 16)
    	_, err := rand.Read(b)
		if err != nil {
			fmt.Println(err)
		}
		nonce := base64.StdEncoding.EncodeToString([]byte(hex.EncodeToString(b[:])))
		fmt.Fprintf(conn, "334 "+nonce+"\r\n")
		credentials, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
		}
		credentials = strings.Trim(credentials, "\r\n")
		data, err := base64.StdEncoding.DecodeString(credentials)
        if err != nil {
                log.Fatal("error:", err)
        }
		credentials = string(data[:])
		hash := md5.Sum([]byte(s.Password+hex.EncodeToString(b[:])))
		if (credentials == hex.EncodeToString(hash[:])) {
			fmt.Fprintf(conn, "235 2.7.0 Authentication Succeeded\r\n")
			return true
		}
		fmt.Fprintf(conn, "535 5.7.8 Authentication credentials invalid\r\n")
		return false
	}
	if (method == "CRAM-MD5") {
		b := make([]byte, 32)
    	_, err := rand.Read(b)
		if err != nil {
			fmt.Println(err)
		}
		challenge := base64.StdEncoding.EncodeToString([]byte(hex.EncodeToString(b[:])))
		fmt.Fprintf(conn, "334 "+challenge+"\r\n")
		credentials, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
		}
		credentials = strings.Trim(credentials, "\r\n")
		data, err := base64.StdEncoding.DecodeString(credentials)
        if err != nil {
                log.Fatal("error:", err)
        }
		credentials = string(data[:])
		secretHash := md5.New()
		secretHash.Write([]byte(s.Password))
		key := secretHash.Sum(nil)

		sig := hmac.New(md5.New, key)
		sig.Write([]byte(challenge))
		if (credentials == hex.EncodeToString(sig.Sum(nil))) {
			fmt.Fprintf(conn, "235 2.7.0 Authentication Succeeded\r\n")
			return true
		}
		fmt.Fprintf(conn, "535 5.7.8 Authentication credentials invalid\r\n")
		return false
	}
	fmt.Fprintf(conn, "504 5.5.4 Unrecognized authentication type\r\n")
	return false
}

func (s *SMTPServer) handleConnection(conn net.Conn) {
	fmt.Fprintf(conn, "220 smtp."+s.Domain+" ESMTP Nexus\r\n")
	
	sender := ""
	var recipients []string
	var is_tls bool
	var is_authenticated bool
	for {
		msg, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
		}
		if (strings.HasPrefix(strings.ToUpper(msg), "QUIT")) {
			fmt.Fprintf(conn, "221 2.0.0 Goodbye\r\n")
			break
		}
		if (strings.HasPrefix(strings.ToUpper(msg), "RSET")) {
			fmt.Fprintf(conn, "250 Ok\r\n")
			sender = ""
			recipients = []string{}
			continue
		}
		if (strings.HasPrefix(strings.ToUpper(msg), "HELO")) {
			fqdn := msg[len("HELO "):]
			fmt.Fprintf(conn, "250 smtp."+s.Domain+" HELO "+fqdn+"\r\n")
			continue
		}
		if (strings.HasPrefix(strings.ToUpper(msg), "EHLO")) {
			fqdn := msg[len("EHLO "):]
			fmt.Fprintf(conn, "250-smtp2."+s.Domain+" EHLO "+fqdn+"\r\n")
			if (is_tls) {
				fmt.Fprintf(conn, "250-AUTH GSSAPI DIGEST-MD5 CRAM-MD5 PLAIN\r\n")
				debounce(s.DebounceNS)
				fmt.Fprintf(conn, "250-SIZE "+strconv.Itoa(s.MaxMsgSize)+"\r\n")
				debounce(s.DebounceNS)
				fmt.Fprintf(conn, "250 HELP\r\n")
			} else {
				fmt.Fprintf(conn, "250-AUTH GSSAPI DIGEST-MD5 CRAM-MD5\r\n")
				debounce(s.DebounceNS)
				fmt.Fprintf(conn, "250-SIZE "+strconv.Itoa(s.MaxMsgSize)+"\r\n")
				debounce(s.DebounceNS)
				fmt.Fprintf(conn, "250-STARTTLS\r\n")
				debounce(s.DebounceNS)
				fmt.Fprintf(conn, "250 HELP\r\n")
			}
			continue
		}
		if (strings.HasPrefix(strings.ToUpper(msg), "HELP")) {
			if (is_tls) {
				fmt.Fprintf(conn, "250-AUTH GSSAPI DIGEST-MD5 CRAM-MD5 PLAIN\r\n")
				debounce(s.DebounceNS)
				fmt.Fprintf(conn, "250-SIZE "+strconv.Itoa(s.MaxMsgSize)+"\r\n")
				debounce(s.DebounceNS)
				fmt.Fprintf(conn, "250 HELP\r\n")
			} else {
				fmt.Fprintf(conn, "250-AUTH GSSAPI DIGEST-MD5 CRAM-MD5\r\n")
				debounce(s.DebounceNS)
				fmt.Fprintf(conn, "250-SIZE "+strconv.Itoa(s.MaxMsgSize)+"\r\n")
				debounce(s.DebounceNS)
				fmt.Fprintf(conn, "250-STARTTLS\r\n")
				debounce(s.DebounceNS)
				fmt.Fprintf(conn, "250 HELP\r\n")
			}
			continue
		}
		if (strings.HasPrefix(strings.ToUpper(msg), "AUTH")) {
			method := msg[len("AUTH "):]
			is_authenticated = s.authenticateSession(conn, is_tls, method)
			continue
		}
		if (strings.HasPrefix(strings.ToUpper(msg), "SIZE")) {
			fmt.Fprintf(conn, "250 Ok: "+strconv.Itoa(s.MaxMsgSize)+"\r\n")
			continue
		}
		if (strings.HasPrefix(strings.ToUpper(msg), "STARTTLS")) {
			fmt.Fprintf(conn, "220 Ready to start TLS\r\n")
			is_tls = true
			continue
		}
		if (strings.HasPrefix(strings.ToUpper(msg), "MAIL FROM")) {
			if (!s.AuthRequired || (s.AuthRequired && is_authenticated)) {
				sender = strings.ToLower(msg[strings.Index(msg, "<")+1:strings.Index(msg, ">")])
				fmt.Fprintf(conn, "250 <"+sender+"> Ok\r\n")
				continue
			}
			fmt.Fprintf(conn, "530 5.7.0 Authentication required\r\n")
			continue
		}
		if (strings.HasPrefix(strings.ToUpper(msg), "RCPT TO")) {
			if (sender == "") {
				fmt.Fprintf(conn, "503 Bad sequence of commands\r\n")
				continue
			}
			if (!s.AuthRequired || (s.AuthRequired && is_authenticated)) {
				recipient := strings.ToLower(msg[strings.Index(msg, "<")+1:strings.Index(msg, ">")])
				fmt.Fprintf(conn, "250 <"+recipient+"> Ok\r\n")
				recipients = append(recipients, recipient)
				continue
			}
			fmt.Fprintf(conn, "530 5.7.0 Authentication required\r\n")
			continue
		}
		if (strings.HasPrefix(strings.ToUpper(msg), "DATA")) {
			if (s.AuthRequired && !is_authenticated) {
				fmt.Fprintf(conn, "530 5.7.0 Authentication required\r\n")
				continue
			}
			if (len(recipients) == 0) {
				fmt.Fprintf(conn, "554 No valid recipients\r\n")
				continue
			}
			fmt.Fprintf(conn, "354 End data with <CRLF>.<CRLF>\r\n")
			var data string
			for {
				msg, err = bufio.NewReader(conn).ReadString('\n')
				if err != nil {
					fmt.Println(err)
				}
				data += msg
				if (strings.HasSuffix(data, "\r\n.\r\n")) {
					break
				}
				data = strings.Replace(data, "\n..", "\n.", -1)
			}
			filtered := s.filterData(conn, data)
			if (filtered != nil) {
				item := QueueItem{sender, recipients, filtered}
				fmt.Println(item)
				s.Queue.Put(item)
				fmt.Fprintf(conn, "250 Ok: queued as "+strconv.Itoa(s.Queue.Size())+"\r\n")
			}
			sender = ""
			recipients = []string{}
		}
	}
	conn.Close()
}

func (s *SMTPServer) Run() {
	server, _ := net.Listen("tcp", s.Host+":"+strconv.Itoa(s.Port))
	fmt.Println("Server running on "+s.Host+":"+strconv.Itoa(s.Port))
	//go s.processItems()
	for {
		conn, _ := server.Accept()
		go s.handleConnection(conn)
	}
}

func debounce(duration int64) {
	start := time.Now().UnixNano()
    for {
		if (time.Now().UnixNano() - start > duration) {
			break
		}
	}
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	port, _ := strconv.Atoi(os.Getenv("PG_PORT"))
	pg := Postgres {
		Host: os.Getenv("PG_HOST"),
		Port: port,
		DBName: os.Getenv("PG_DBNAME"),
		User: os.Getenv("PG_USER"),
		Password: os.Getenv("PG_PASS"),
	}
	port, _ = strconv.Atoi(os.Getenv("SMTP_PORT"))
	server := SMTPServer {
		Domain: os.Getenv("SMTP_DOMAIN"), 
		Host: os.Getenv("SMTP_HOST"), 
		Port: port, 
		Password: os.Getenv("SMTP_PASS"), 
		AuthRequired: true, 
		MaxMsgSize: 14680064, 
		DebounceNS: 500_000,
		DB: pg,
	}
	server.Run()
}
