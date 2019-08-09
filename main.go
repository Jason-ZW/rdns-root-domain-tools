package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/route53"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	// in order to make build through
	_ "github.com/go-sql-driver/mysql"
)

const (
	maxOpenConnections = 2000
	maxIdleConnections = 1000
)

var (
	VERSION = "v0.0.1"
	DATE    string
)

type DBRecord struct {
	ID        int64         `db:"id"`
	Fqdn      string        `db:"fqdn"`
	Type      int           `db:"type"`
	Content   string        `db:"content"`
	CreatedOn int64         `db:"created_on"`
	UpdatedOn sql.NullInt64 `db:"updated_on"`
	TID       int64         `db:"tid"`
}

func init() {
	cli.VersionPrinter = versionPrinter
}

func main() {
	app := cli.NewApp()
	app.Author = "Rancher Labs, Inc."
	app.Before = beforeFunc
	app.EnableBashCompletion = true
	app.Name = os.Args[0]
	app.Usage = fmt.Sprintf("migrate RDNS from 0.4.x to 0.5.x(%s)", DATE)
	app.Version = VERSION
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:   "debug, d",
			EnvVar: "DEBUG",
			Usage:  "used to set debug mode.",
		},
		cli.StringFlag{
			Name:   "dsn",
			EnvVar: "DSN",
			Usage:  "used to set data source name.",
		},
		cli.StringFlag{
			Name:   "aws_hosted_zone_id",
			EnvVar: "AWS_HOSTED_ZONE_ID",
			Usage:  "used to set aws hosted zone ID.",
		},
		cli.StringFlag{
			Name:   "aws_access_key_id",
			EnvVar: "AWS_ACCESS_KEY_ID",
			Usage:  "used to set aws access key ID.",
		},
		cli.StringFlag{
			Name:   "aws_secret_access_key",
			EnvVar: "AWS_SECRET_ACCESS_KEY",
			Usage:  "used to set aws secret access key.",
		},
	}

	app.Action = func(ctx *cli.Context) error {
		if err := appMain(ctx); err != nil {
			return err
		}
		return nil
	}

	if err := app.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func appMain(ctx *cli.Context) error {
	if ctx.Bool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}

	setEnvironments(ctx)

	c := credentials.NewEnvCredentials()

	s, err := session.NewSession()
	if err != nil {
		return err
	}

	svc := route53.New(s, &aws.Config{
		Credentials: c,
	})

	zone, err := svc.GetHostedZone(&route53.GetHostedZoneInput{
		Id: aws.String(os.Getenv("AWS_HOSTED_ZONE_ID")),
	})
	if err != nil {
		return err
	}

	db, err := sql.Open("mysql", ctx.String("dsn"))
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(maxOpenConnections)
	db.SetMaxIdleConns(maxIdleConnections)

	defer db.Close()

	st, err := db.Prepare("SELECT * FROM record_a where fqdn like ?")
	if err != nil {
		return err
	}
	defer st.Close()

	rows, err := st.Query("\\\\052.%")
	if err != nil {
		return err
	}

	for rows.Next() {
		temp := &DBRecord{}
		if err := rows.Scan(&temp.ID, &temp.Fqdn, &temp.Type, &temp.Content, &temp.CreatedOn, &temp.UpdatedOn, &temp.TID); err != nil {
			return err
		}

		sst, err := db.Prepare("SELECT fqdn FROM record_a where fqdn = ?")
		if err != nil {
			return err
		}
		defer sst.Close()

		name := strings.Split(temp.Fqdn, "\\052.")[1]
		var foundName string
		if err := sst.QueryRow(name).Scan(&foundName); err != nil && err != sql.ErrNoRows {
			return err
		}

		if err == sql.ErrNoRows || foundName == "" {
			// insert record to database
			it, err := db.Prepare("INSERT INTO record_a (fqdn, type, content, created_on, tid) VALUES (?, ?, ?, ?, ?)")
			if err != nil {
				return err
			}
			defer it.Close()

			_, err = it.Exec(name, temp.Type, temp.Content, temp.CreatedOn, temp.TID)
			if err != nil {
				return err
			}
			// upsert record to route53.
			input := route53.ListResourceRecordSetsInput{
				HostedZoneId:    zone.HostedZone.Id,
				StartRecordName: aws.String(temp.Fqdn),
				StartRecordType: aws.String("A"),
			}

			output, err := svc.ListResourceRecordSets(&input)
			if err != nil {
				return err
			}

			for _, rs := range output.ResourceRecordSets {
				recordName := aws.StringValue(rs.Name)
				if strings.TrimSuffix(recordName, ".") == "*."+name || strings.TrimSuffix(recordName, ".") == "\\052."+name {
					rs.Name = aws.String(name)
					upsert := route53.ChangeResourceRecordSetsInput{
						HostedZoneId: zone.HostedZone.Id,
						ChangeBatch: &route53.ChangeBatch{
							Changes: []*route53.Change{
								{
									Action:            aws.String("UPSERT"),
									ResourceRecordSet: rs,
								},
							},
						},
					}

					if _, err := svc.ChangeResourceRecordSets(&upsert); err != nil {
						return err
					}
				}
			}

		}

	}

	return nil
}

func beforeFunc(c *cli.Context) error {
	if os.Getuid() != 0 {
		logrus.Fatalf("%s: need to be root", os.Args[0])
	}
	return nil
}

func versionPrinter(c *cli.Context) {
	if _, err := fmt.Fprintf(c.App.Writer, VERSION); err != nil {
		logrus.Error(err)
	}
}

func setEnvironments(c *cli.Context) {
	if c.GlobalBool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}

	os.Setenv("DSN", c.String("dsn"))
	os.Setenv("AWS_HOSTED_ZONE_ID", c.String("aws_hosted_zone_id"))
	os.Setenv("AWS_ACCESS_KEY_ID", c.String("aws_access_key_id"))
	os.Setenv("AWS_SECRET_ACCESS_KEY", c.String("aws_secret_access_key"))
}