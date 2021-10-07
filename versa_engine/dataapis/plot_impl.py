def hex_to_RGB(hex):
    ''' "#FFFFFF" -> [255,255,255] '''
    # Pass 16 to the integer function for change of base
    return [int(hex[i:i+2], 16) for i in range(1,6,2)]

def RGB_to_hex(RGB):
    ''' [255,255,255] -> "#FFFFFF" '''    # Components need to be integers for hex to make sense
    RGB = [int(x) for x in RGB]
    return "#"+"".join(["0{0:x}".format(v) if v < 16 else 
                        "{0:x}".format(v) for v in RGB])


def color_dict(gradient):


    ''' Takes in a list of RGB sub-lists and returns dictionary of 
        colors in RGB and hex form for use in a graphing function 
        defined later on '''
    return {"hex":[RGB_to_hex(RGB) for RGB in gradient],
            "r":[RGB[0] for RGB in gradient],
            "g":[RGB[1] for RGB in gradient],
            "b":[RGB[2] for RGB in gradient]}

fact_cache = {}
def fact(n):
    ''' Memoized factorial function '''
    try:
        return fact_cache[n]
    except(KeyError):
        if n == 1 or n == 0:
            result = 1
        else:
            result = n*fact(n-1)
        fact_cache[n] = result
        return result


    
def bernstein(t,n,i):
    ''' Bernstein coefficient '''
    binom = fact(n)/float(fact(i)*fact(n - i))
    return binom*((1-t)**(n-i))*(t**i)

def bezier_gradient(colors, num_out=10):
    #source:  http://bsou.io/p/3
    RGB_list = [hex_to_RGB(color) for color in colors]
    n = len(RGB_list) - 1
    def bezier_interp(t):
        summands = [ map(lambda x: int(bernstein(t,n,i)*x), c)
                     for i, c in enumerate(RGB_list)] 
        out = [0,0,0]
        for vector in summands:
            for c in range(3): 
                out[c] += vector[c]
        return out
    gradient = [bezier_interp(float(t)/(num_out-1)) for t in range(num_out)]
    return {"gradient": color_dict(gradient), 
            "control": color_dict(RGB_list)}


def bezier_gradient(colors, num_out=10):
    #source: http://bsou.io/p/3
    RGB_list = [hex_to_RGB(color) for color in colors]
    n = len(RGB_list) - 1
    def bezier_interp(t):
        summands = [ map(lambda x: int(bernstein(t,n,i)*x), c)
                     for i, c in enumerate(RGB_list)] 
        out = [0,0,0]
        for vector in summands:
            for c in range(3): 
                out[c] += vector[c]
        return out
    gradient = [bezier_interp(float(t)/(num_out-1)) for t in range(num_out)]
    return {"gradient": color_dict(gradient), 
            "control": color_dict(RGB_list)}


def pick_colors_from_anchors(anchors, num_gradients):
    #anchors = ['#66FF66', '#660066', '#993333']
    gradient = bezier_gradient(anchors, num_gradients)
    colors =  gradient['gradient']['hex']
    return colors #color values in hex
