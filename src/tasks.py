from ticketParser.TicketParser import TicketParser


if __name__ == "__main__":
    parser = TicketParser("../data/ticket/day1.csv",  all=True)
    parser.distribute_records()
