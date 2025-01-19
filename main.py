import csv
import pytz

import my_io
import price_history.queries as price_queries
import utils

from constants import DATE_FORMAT_FROM_STAKE_TAX, DATE_FORMAT_TO_KRYPTOSEKKEN, DATE_FORMAT_FILE_NAME
from datetime import datetime
from download_tx_details import get_burnt_nft_id
from enums import KryptoSekkenType, SolanaTxType, Source
from typing import Dict, List, Optional
from price_history.price_history_csv_io import update_price_history, download_usd_nok_price_history, get_ticker_by_token_address

def get_source(csv_reader: csv.reader) -> Source:
    try:
        header = next(csv_reader)
        if header and len(header) == 13 and header[10] == "url" and header[11] == "exchange":
            first_row = next(csv_reader)
            if first_row and "solana" in first_row[10] and "solana_blockchain" == first_row[11]:
                return Source.SOLANA
    except:
        pass
    return Source.UNKNOWN

def convert_date_format(date_str: str, source: Source) -> str:
    match source:
        case Source.SOLANA:
            dt = datetime.strptime(date_str, DATE_FORMAT_FROM_STAKE_TAX)   # 2024-04-08 08:49:04
            dt_utc = dt.replace(tzinfo=pytz.UTC)
            oslo_tz = pytz.timezone('Europe/Oslo')
            dt_oslo = dt_utc.astimezone(oslo_tz)
            return dt_oslo.strftime(DATE_FORMAT_TO_KRYPTOSEKKEN)
        case _:
            return ""

def make_notat(comment: str, tx_id: str) -> str:
    if len(comment) == 0 or comment.isspace():
        return tx_id
    else:
        return comment + ', ' + tx_id

def convert_transfer(
        timestamp:str,
        received_amount: str,
        received_currency: str,
        sent_amount: str, 
        sent_currency: str, 
        fee: str, 
        fee_currency: str, 
        comment: str, 
        txid: str, 
        exchange: str) -> List[str]:
    
    type = KryptoSekkenType.OVERFORING_UT.value if received_amount == "" and sent_amount != "" else KryptoSekkenType.OVERFORING_INN.value
    return [convert_date_format(timestamp, Source.SOLANA), 
            type, 
            received_amount, 
            received_currency, 
            sent_amount, 
            sent_currency, 
            fee, 
            fee_currency, 
            exchange, 
            make_notat(comment, txid)]

def convert_spending(
        timestamp:str,
        received_amount: str,
        received_currency: str,
        sent_amount: str, 
        sent_currency: str, 
        fee: str, 
        fee_currency: str, 
        comment: str, 
        txid: str, 
        exchange: str) -> List[str]:
    
    sent_amount = sent_amount[0:19]     #Kryptosekken cannot handle numbers that are 21 characters long

    return [convert_date_format(timestamp, Source.SOLANA), 
            KryptoSekkenType.FORBRUK.value, 
            received_amount, 
            received_currency, 
            sent_amount, 
            sent_currency, 
            fee, 
            fee_currency, 
            exchange, 
            make_notat(comment, txid)]

def is_valid_ticker(ticker: str) -> bool:
    return len(ticker) < 10 and all(c.isupper() or c.isnumeric for c in ticker)

def clean_currency_code(currency: str) -> str:
    return currency.replace("$", "")    # replaces $WIF with WIF. Kryptosekken doesn't like $WIF

def main() -> None:

    update_price_history()
    try:
        download_usd_nok_price_history()
    except Exception as e:
        print(e)

    with open('default.csv', 'r') as csvfile_in:
        csv_reader = csv.reader(csvfile_in)
        source = get_source(csv_reader)
        
        csvfile_in.seek(0)
        next(csv_reader)    #position on the first row with data

        output_fields = ["Tidspunkt", "Type", "Inn", "Inn-Valuta", "Ut", "Ut-Valuta", "Gebyr", "Gebyr-Valuta", "Marked", "Notat"]
        output_filename = "til_kryptosekken {}.csv".format(datetime.now().strftime(DATE_FORMAT_FILE_NAME))

        with open(output_filename, 'w', newline='') as csvfile_out:
            # creating a csv writer object
            csv_writer = csv.writer(csvfile_out)
        
            # writing the fields
            csv_writer.writerow(output_fields)
        
            num_trade = 0
            num_orca_nft = 0
            num_transfer = 0
            num_airdrop = 0
            num_spend = 0
            num_jupiter_dca_open = 0    
            num_jupiter_limit_open = 0
            num_staking = 0
            num_unkown = 0

            txid_to_nft_id_map: Dict[str, Optional[str]] = my_io.load_tx_nft_dictionary_from_file()
            tx_nft_map_new_entries = 0

            # Iterate over each row in the CSV file
            for (date_str_in, tx_type, received_amount, received_currency, 
                sent_amount, sent_currency, fee, fee_currency, comment, 
                txid, url, exchange, wallet_address) in csv_reader:

                received_currency = clean_currency_code(received_currency)
                sent_currency = clean_currency_code(sent_currency)
                fee_currency = clean_currency_code(fee_currency)

                # Each row is a list containing the values in the row
                if (received_amount == "1.0" and not is_valid_ticker(received_currency) or 
                    sent_amount == "1.0" and not is_valid_ticker(sent_currency)):
                    num_orca_nft += 1

                    continue # this is the Orca NFT that belongs to the Orca deposit. Not interesting from a tax perspective
                
                orig_received_currency = received_currency
                if 40 < len(received_currency):
                    received_currency = get_ticker_by_token_address(received_currency)
                if 40 < len(sent_currency):
                    sent_currency = get_ticker_by_token_address(sent_currency)
                    
                date_str_out = convert_date_format(date_str_in, source)
                if tx_type == SolanaTxType.TRADE.value:
                    csv_writer.writerow([date_str_out, KryptoSekkenType.HANDEL.value, received_amount, received_currency, sent_amount, sent_currency, fee, fee_currency, exchange, make_notat(comment, txid)])
                    num_trade += 1
                    continue

                if tx_type == SolanaTxType.TRANSFER.value:
                    if utils.is_round_number(received_amount) and 40 < len(orig_received_currency):
                        num_airdrop += 1
                        csv_writer.writerow((convert_date_format(date_str_in, Source.SOLANA), 
                            KryptoSekkenType.ERVERV.value, 
                            received_amount, 
                            received_currency, 
                            sent_amount, 
                            sent_currency, 
                            fee, 
                            fee_currency, 
                            exchange, 
                            make_notat(comment, txid)))
                        
                    else:
                        csv_writer.writerow(convert_transfer(date_str_in, received_amount, received_currency, sent_amount, sent_currency, fee, fee_currency, comment, txid, exchange))
                        num_transfer += 1

                    continue

                if tx_type == SolanaTxType.SPEND.value:
                    csv_writer.writerow(convert_spending(date_str_in, received_amount, received_currency, sent_amount, sent_currency, fee, fee_currency, comment, txid, exchange))
                    num_spend += 1
                    continue

                if tx_type == SolanaTxType.JUPITER_DCA_OPEN.value:
                    num_jupiter_dca_open += 1
                    continue

                if tx_type == SolanaTxType.JUPITER_LIMIT_OPEN.value:
                    num_jupiter_limit_open += 1
                    continue

                if tx_type == SolanaTxType.STAKING.value:
                    num_staking += 1
                    continue

                if tx_type == SolanaTxType.UNKNOWN.value:
                    num_unkown += 1
                    if received_amount == "" and received_currency == "" and sent_amount == "" and sent_currency == "":
                        csv_writer.writerow([date_str_out, KryptoSekkenType.FORVALTNINGSKOSTNAD.value, 
                                            "",    # Inn 
                                            "",    # Inn-Valuta
                                            fee,    # Ut
                                            fee_currency,    # Ut-Valuta
                                            "",    # Gebyr
                                            "",    # Gebyr-Valuta
                                            exchange, make_notat(comment, txid)])
                        continue

                    if received_amount == "" and received_currency == "" and sent_amount != "" and sent_currency != "":
                        comment = "Orca pool deposit"
                        csv_writer.writerow([date_str_out, KryptoSekkenType.OVERFORING_UT.value, 
                                            "",     # Inn
                                            "",     # Inn-Valuta
                                            sent_amount, sent_currency, fee, fee_currency, exchange, make_notat(comment, txid)])
                        continue

                    if received_amount != "" and received_currency != "":
                        
                        burnt_nft_id = None
                        ut = ut_valuta = ""

                        # check if we can find a burnt NFT for the tx
                        if txid in txid_to_nft_id_map:  # offline
                            burnt_nft_id = txid_to_nft_id_map[txid]
                        else:
                            burnt_nft_id = get_burnt_nft_id(txid)   # online
                            txid_to_nft_id_map[txid] = burnt_nft_id
                            tx_nft_map_new_entries += 1

                        if burnt_nft_id != None:
                            tx_type = KryptoSekkenType.OVERFORING_INN.value
                            comment = "Orca pool withdrawal"
                            ut = ut_valuta = ""

                        else:   # not an orca nft
                            if utils.is_round_number(received_amount):
                                tx_type = KryptoSekkenType.ERVERV.value
                                comment = "Airdrop"
                                ut_valuta = "NOK"
                                usd = price_queries.price_at_time(received_currency, date_str_in)    #UTC time
                                ut = price_queries.convert_usd_to_nok(usd, date_str_in)

                            elif price_queries.is_price_history_locally_available(received_currency):   # tokens price is not available by Kryptosekken
                                tx_type = KryptoSekkenType.INNTEKT.value
                                comment = "Orca pool harvest"
                                ut_valuta = "NOK"
                                usd = price_queries.price_at_time(received_currency, date_str_in)    #UTC time
                                ut = price_queries.convert_usd_to_nok(usd, date_str_in)

                            else:   # normal, bigger token whose data is available on Kryptosekken and we get a normal ticker, not its address
                                tx_type = KryptoSekkenType.INNTEKT.value
                                comment = "Orca pool harvest"
                                ut = ut_valuta = ""

                        utstr = str(ut)[0:19]   # makes sure it is not longer than 20 characters
                        csv_writer.writerow([date_str_out, tx_type, received_amount, received_currency, 
                                             utstr, ut_valuta, fee, fee_currency, exchange, make_notat(comment, txid)])
                        continue

                    print("Unknown, not written!")

                raise RuntimeError(f"tx_type \"{tx_type}\" is not handled")
            
            if tx_nft_map_new_entries > 0:
                my_io.save_tx_nft_dictionary_to_file(txid_to_nft_id_map)

            print(tx_nft_map_new_entries, "tx ids were checked on Solana.fm")
            print("num_trade:", num_trade)
            print("num_orca_nft:", num_orca_nft)
            print("num_transfer:", num_transfer)
            print("num_airdrop:", num_airdrop)
            print("num_spend:", num_spend)
            print("num_jupiter_dca_open:", num_jupiter_dca_open)
            print("num_jupiter_limit_open:", num_jupiter_limit_open)
            print("num_staking:", num_staking)
            print("num_unkown:", num_unkown)
            print("sum_in:", 
                  num_trade + 
                  num_orca_nft + 
                  num_transfer + 
                  num_spend + 
                  num_jupiter_dca_open + 
                  num_jupiter_limit_open + 
                  num_staking + 
                  num_unkown)
            print("sum_out:", num_trade + num_transfer + num_spend + num_unkown)
                
    print("Conversion finished successfully")

if __name__ == "__main__":
    main()