// load data
const chalk = require('chalk');
const { log, time, timeEnd } = console;

const x = require('x-ray');
const tableScraper = require('table-scraper');
const async = require('async');
const GoogleSpreadsheet = require('google-spreadsheet');

const config = require('./config.json');
const creds = require('./config.credentials.json');

const doc = new GoogleSpreadsheet(config.sheetsKey);

getData();

async function getData() {
    await auth(doc, creds);
    const sheet = await getInfo(doc);
    const rows = await getRows(sheet);
    for (const row of rows) {
        const minutes = await getMinutes(row);
        if (minutes) {
            await sleep();
        }
    }
    // log(rows);
}

function auth(doc, creds) {
    return new Promise((resolve, reject) => {
        doc.useServiceAccountAuth(creds, resolve);
    });
}

function getInfo(doc) {
    return new Promise((resolve, reject) => {
        doc.getInfo((err, info) => {
            if (err) {
                log('An error occurred')
                reject();
            }
            log(chalk.blue(`Loaded spreadsheet: ${info.title}`));
            resolve(info.worksheets[0]);
        })
    });
}

function getRows(sheet, offset = 0, limit = null) {
    return new Promise((resolve, reject) => {
        sheet.getRows({
            offset,
            limit // just do a few rows at a time for now so we don't kill mlssoccer.com
        }, (err, rows) => {
            if (err) {
                log(err);
                reject(err);
            } else {
                resolve(rows);
            }
        });
    })
}

function getMinutes(row) {
    return new Promise(async (resolve) => {
        if (row.url && row.m2017 === '') {
            log(chalk.yellow(`Fetching minutes for ${row.player}`))
            const yearData = await fetchMinutes(row.url);
            if (yearData && Object.keys(yearData).length) {
                // process year data
                row.m2017 = false;
                Object.keys(yearData).forEach(year => {
                    row[`m${year}`] = yearData[year].MINS;
                });
            } else if (yearData) {
                row.m2017 = false;
            }
            log(chalk.white.bgGreen.bold(`Saving for ${row.player}: ${row.m2017}`));
            row.save();
            resolve(yearData);
        } else {
            log(chalk.green(`Minutes for ${row.player}: ${row.m2017}`))
            resolve();
        }
    })
}

function fetchMinutes(url) {
    return new Promise((resolve, reject) => {
        log(chalk.white.bgGreen(`Performing fetch`));
        time('fetch')
        tableScraper.get(url).then((tables) => {
            timeEnd('fetch')
            if (Array.isArray(tables)) {
                yearsData = {};
                // find the right table
                for (let table in tables) {
                    yearsData = tables[0].reduce((acc, row) => {
                        if (row && !row.School && row.Year && row.Year !== 'Career Totals') {
                            acc[row.Year] = row;
                        }
                        return acc;
                    }, yearsData);
                }
                resolve(yearsData);
            } else {
                resolve({});
            }
        }).catch(err => {
            timeEnd('fetch')
            log(chalk.white.bgRed(url));
            log(err);
            resolve();
        })
    });
}

async function sleep(timeout = 500) {
    return new Promise(resolve => setTimeout(resolve, timeout));
}
