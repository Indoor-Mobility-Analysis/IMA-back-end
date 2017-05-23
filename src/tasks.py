from ticketParser.TicketParser import TicketParser


if __name__ == "__main__":
    parser = TicketParser("../data/ticket.csv",  all=True)
    parser.distribute_records()
