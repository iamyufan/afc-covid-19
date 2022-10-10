# afc-covid-data

Using GitHub Actions to fetch daily COVID-19 data at Amazon Fulfillment Center (AFC) in the US

[project repo](https://github.com/SaintLaurentConsulting/Durham)

## API

[Heroku App](https://afc-covid-data.herokuapp.com/)

- [/data](https://afc-covid-data.herokuapp.com/data): get the whole json data
- /center/<center_id>: get the json data for a particular center
- /state/<state_id>: get the json data for a particular state
- /date/\<YYYY-MM-DD\>: get the json data for a particular date
