# Сгенерировано с помощью https://www.asciiart.eu/text-to-ascii-art
# ANSI Shadow

from colorama import Fore


logo = """
 ███████╗ ██████╗ ██╗                                    
 ██╔════╝██╔═══██╗██║                                    
 ███████╗██║   ██║██║                                    
 ╚════██║██║▄▄ ██║██║                                    
 ███████║╚██████╔╝███████╗                               
 ╚══════╝ ╚══▀▀═╝ ╚══════╝                               
                                                        
 ██████╗ ██╗██████╗ ███████╗██╗     ██╗███╗   ██╗███████╗
 ██╔══██╗██║██╔══██╗██╔════╝██║     ██║████╗  ██║██╔════╝
 ██████╔╝██║██████╔╝█████╗  ██║     ██║██╔██╗ ██║█████╗  
 ██╔═══╝ ██║██╔═══╝ ██╔══╝  ██║     ██║██║╚██╗██║██╔══╝  
 ██║     ██║██║     ███████╗███████╗██║██║ ╚████║███████╗
 ╚═╝     ╚═╝╚═╝     ╚══════╝╚══════╝╚═╝╚═╝  ╚═══╝╚══════╝
"""   



logo_colored = ""
version = " v.2025-10-28-00:35"

for ch in list(logo):
    if ch in ['╝', '═', '║', '╔', '╚', '╗']:
        ch_colored = Fore.GREEN + ch + Fore.RESET
    elif ch in ('█', '▄', '▀'):    
        ch_colored = Fore.CYAN + chr(9619) + Fore.RESET
    else:
        ch_colored = ch
    logo_colored += ch_colored

version_colored = Fore.GREEN + version + Fore.RESET    

logo_colored = logo_colored + version_colored