#!/usr/bin/env python
"""\
Author: Lucas Boulé, 2023-10-06
Send an email with a log file attached.
Usage:
send_log_mail(log_path, started_at, receiver_email, n_lines_printed=20)
"""
import os
import sys
from pathlib import Path
import smtplib
from pathlib import Path
import datetime
import re
from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
sys.path.append(str(Path(os.getcwd())))
from utils.Toolbox_lib import safe_cmd
from module.env import *


"""
Function to send an email with attach files
input: subject, text, sender, receiver, logger, directory files, file
ouput: 
"""

def send_email(subject: str, html : str, sender :str, receiver , rep_file=None , file=None):

    # on crée deux éléments MIMEText 
    html_mime = MIMEText(html, 'html')
    msg =  MIMEMultipart("alternative")
    msg['Subject'] = subject
    msg['From'] = sender # pour l'affichage
    msg['To'] = receiver # pour l'affichage

    # envoi du mail avec l'expediteur et les destinataires
    if rep_file:       
        for fichier in rep_file.glob(f"**/*_{date}.csv"):        
            part = MIMEBase('application', "octet-stream")
            try:
                f = open(fichier,"rb")
                part.set_payload(f.read())
                f.close()

            except:
                coderr = "%s" % sys.exc_info()[1]
                lg.error('échec à la lecture du fichier en pièce jointe %s', sys.exc_info()[1])  
                raise ValueError ("échec à la lecture du fichier en pièce jointe (" + coderr + ")")
                    
            part.add_header('Content-Disposition', 'attachment', filename="%s" % os.path.basename(fichier))
            msg.attach(part)
            # on attache ces deux éléments 
            msg.attach(html_mime)

    if file:
        
        try:
            with open(file, 'rb') as attachment:
                part = MIMEBase('application', "octet-stream")
                part.set_payload(attachment.read())
                part.add_header(
                "Content-Disposition",
                f"attachment; filename= {file.name}",
            )
            

        except:
            coderr = "%s" % sys.exc_info()[1]
            lg.error('échec à la lecture du fichier en pièce jointe %s', sys.exc_info()[1])  
            raise ValueError ("échec à la lecture du fichier en pièce jointe (" + coderr + ")")
                
        part.add_header('Content-Disposition', 'attachment', filename="%s" % os.path.basename(file))
        msg.attach(part)
        # on attache ces deux éléments 
        msg.attach(html_mime)

       
    try:
        # le nom du serveur de mail
        smtp = smtplib.SMTP('mta.partage.renater.fr') 
        smtp.sendmail(sender,
                    [receiver],
                    msg.as_string())
        lg.info("email envoyé avec succès")
    except Exception as e:
        lg.error("échec de l'envoie du mail %s", sys.exc_info()[1]) 
    finally:
        smtp.quit()


def write_message(sender_email: str, receiver_email: str, subject: str, body: str, log_attachment_path = None, result_attachment_path = None):
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "text"))

    if log_attachment_path is not None:
        with open(log_attachment_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {log_attachment_path.name}",
            )
            msg.attach(part)

    if result_attachment_path is not None:
        
        for fichier in result_attachment_path.glob(f"**/*_{date}.csv"):
            part = MIMEBase('application', "octet-stream")
            try:
                f = open(fichier,"rb")
                part.set_payload(f.read())
                f.close()
            except:
                coderr = "%s" % sys.exc_info()[1]
                db_logger.error('échec à la lecture du fichier en pièce jointe %s', sys.exc_info()[1])  
                raise ValueError ("échec à la lecture du fichier en pièce jointe (" + coderr + ")")                    
            part.add_header('Content-Disposition', 'attachment', filename="%s" % os.path.basename(fichier))
            msg.attach(part)
            # on attache ces deux éléments 
    return msg


def send_mail(sender_email: str, receiver_email: str, message: EmailMessage, smtp_server: str, port: int = 25):
    with smtplib.SMTP(smtp_server, port) as server:
        server.sendmail(sender_email, receiver_email, message.as_string())


def send_log_mail(log_path: Path, result_path: Path, receiver_email: str, message_email: str, started_at=datetime.datetime.now(), n_lines_printed: int = 20, subject_prefix: str = "LOG_DEV"):
    # server firefox
    # myip, _ = safe_cmd("hostname -I | cut -d' ' -f1")
    # if myip.startswith('193.48'):
    #     smtp_server = 'mta.partage.renater.fr'
    # elif myip.startswith('134.158'):
    #     smtp_server = 'smtp.in2p3.fr'
    # else:
    #     raise ValueError(f"Unknown IP: {myip}, no SMTP server to map it to.")

    port = 25  # for starttls
    sender_email = 'bertrand.ntep@eurofidai.org'
    smtp_server = 'mta.partage.renater.fr'

    with open(log_path, 'r') as f:
        log = f.read()
        error_logs = re.findall(r'ERROR:.*', log)
        
    if len(error_logs) > 0:
        details = error_logs[0]
        subject = f"ERROR - {log_path.name}"
    else:
        subject = f"SUCCESS - {log_path.name}"
        details = subject
    
    process_time = datetime.datetime.now() - started_at

    summary = message_email
    summary += f"Execution time: {datetime.datetime.now()} \n\n"
    summary += f"Time taken: {process_time} \n"
    summary += f"Execution path: {Path.cwd()} \n"
    summary += f"Execution file: {Path(__file__)} \n"
    summary += f"Log path: {log_path} \n"
    summary += f"Function: {log_path.stem} \n\n"
    summary += f"Details: {details} \n\n"
    summary += f"Detected errors:" + safe_cmd(f"grep -e 'ERREUR' -e 'ERROR' {log_path} | cut -c 28- | sort | uniq\n\n")[0]
    
    # Include the last n lines of the log if exists
    if len(log) > 0:
        nb_lines = len(log.split('\n'))
        nb_lines_printed = min(nb_lines, n_lines_printed)
        summary += f"\n\nLast {nb_lines_printed}/{nb_lines} lines of {log_path.absolute()}: \n"
        summary += '\n'.join(log.split('\n')[-min(len(log.split('\n')), n_lines_printed):])

    msg = write_message(sender_email, receiver_email, f"{subject_prefix}: " + subject.lower(), summary, log_path, result_path)
    
    # Convert MIMEMultipart to EmailMessage
    # email_message = EmailMessage()
    # email_message.set_content(msg.as_string())

    try:
        send_mail(sender_email, receiver_email, msg, smtp_server, port)
    
    except smtplib.SMTPSenderRefused:
        print("SMTPSenderRefused: too large attachment, retrying without attachment.")
        # remove attachement and try again
        msg = write_message(sender_email, receiver_email, f"{subject_prefix}: " + subject.lower(), summary, None, None)
        send_mail(sender_email, receiver_email, msg, smtp_server, port)

   

if __name__ == "__main__":
    send_log_mail(Path('logfile.log'), receiver_email='lucas.boule@eurofidai.org')
